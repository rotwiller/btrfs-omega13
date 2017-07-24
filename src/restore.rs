use std::collections::HashSet;
use std::error::Error;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs as unix_fs;

use btrfs::diskformat::*;

use libc;

use output::Output;

use super::arguments::*;
use super::indexed_filesystem::*;

pub fn restore (
	output: & Output,
	command: RestoreCommand,
) -> Result <(), String> {

	// open filesystem

	let mmap_devices =
		BtrfsMmapDeviceSet::open (
			& command.paths,
		) ?;

	let mut devices =
		mmap_devices.devices () ?;

	let filesystem =
		BtrfsFilesystem::open_try_backups (
			output,
			& devices,
		) ?;

	// locate files to restore

	let root_item =
		filesystem.root_item (
			command.subvolume_id,
		).ok_or (

			format! (
				"Subvolume not found: {}",
				command.subvolume_id)

		) ?;

	let filesystem_tree =
		filesystem.filesystem_tree (
			command.subvolume_id,
		).ok_or (

			format! (
				"Subvolume not found: {}",
				command.subvolume_id)

		) ?;

	let mut child_object_id =
		root_item.root_object_id ();

	let mut child_type =
		BTRFS_FT_DIR;

	for path_part in command.source.iter () {

		if path_part == "/" {
			continue;
		}

		let dir_item_entry =
			filesystem_tree.dir_item_entry (
				child_object_id,
				path_part.as_bytes (),
			).ok_or (

				format! (
					"Path not found: {}",
					command.source.to_string_lossy ())

			) ?;

		child_object_id =
			dir_item_entry.child_object_id ();

		child_type =
			dir_item_entry.child_type ();

	}

	let output_job =
		output_job_start! (
			output,
			"Restoring files");

	restore_item (
		output,
		& filesystem,
		& filesystem_tree,
		child_type,
		child_object_id,
		& command.source,
		& command.target,
	) ?;

	output_job.complete ();

	// return

	Ok (())

}

fn restore_item <'a> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	child_type: u8,
	child_object_id: u64,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	match child_type {

		BTRFS_FT_REG_FILE =>
			restore_regular_file (
				output,
				filesystem,
				filesystem_tree,
				child_object_id,
				& source,
				& target,
			) ?,

		BTRFS_FT_DIR =>
			restore_directory (
				output,
				filesystem,
				filesystem_tree,
				child_object_id,
				& source,
				& target,
			) ?,

		BTRFS_FT_SYMLINK =>
			restore_symlink (
				output,
				filesystem,
				filesystem_tree,
				child_object_id,
				& source,
				& target,
			) ?,

		_ =>
			output_message! (
				output,
				"Can't restore item {} of type: {}",
				source.to_string_lossy (),
				child_type),

	}

	Ok (())

}

fn restore_children <'a> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	directory_id: u64,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	// iterate children

	for dir_index
	in filesystem_tree.dir_indexes (
		directory_id,
	) {

		let source =
			source.join (
				OsString::from_vec (
					dir_index.name ().to_vec ()));

		let target =
			target.join (
				OsString::from_vec (
					dir_index.name ().to_vec ()));

		if let Err (error) =
			restore_item (
				output,
				filesystem,
				filesystem_tree,
				dir_index.child_type (),
				dir_index.child_object_id (),
				& source,
				& target,
			) {

			output_message! (
				output,
				"Error restoring {}: {}",
				target.to_string_lossy (),
				error);

		}

	}

	Ok (())

}

fn restore_regular_file <'a> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	object_id: u64,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	output_message! (
		output,
		"F {}",
		target.to_string_lossy ());

	let inode_item =
		filesystem_tree.inode_item (
			object_id,
		).ok_or (

			format! (
				"Error finding inode {}",
				object_id)

		) ?;

	// create file

	let mut file =
		File::create (
			target,
		).map_err (
			|error|

			format! (
				"Error creating {}: {}",
				target.to_string_lossy (),
				error.description ())

		) ?;

	// restore contents

	restore_file_contents (
		output,
		filesystem,
		filesystem_tree,
		& inode_item,
		& mut file,
		source,
		target,
	) ?;

	// set metadata

	restore_metadata (
		output,
		& inode_item,
		source,
		target,
		false,
	) ?;

	// return

	Ok (())

}

fn restore_file_contents <'a, FileType: Write + Seek> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	inode_item: & 'a BtrfsInodeItem <'a>,
	file: & mut FileType,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	let mut file_position: u64 = 0;

	for extent_data
	in filesystem_tree.extent_datas (
		inode_item.object_id (),
	) {

		if extent_data.offset () != file_position {

			return Err (
				format! (
					"Extent data position error creating {}: expected 0x{:x}, \
					got 0x{:x}",
					target.to_string_lossy (),
					file_position,
					extent_data.offset ()));

		}

		match restore_extent_data (
			output,
			filesystem,
			filesystem_tree,
			& extent_data,
			inode_item.st_size () - file_position,
			file,
			source,
			target,
		) {

			Ok (bytes_restored) =>
				file_position +=
					bytes_restored,

			Err (error) => {

				output_message! (
					output,
					"Error writing {} @ 0x{:x}: {}",
					target.to_string_lossy (),
					file_position,
					error);

				return Ok (())

			},

		}

		if file_position > inode_item.st_size () {
			break;
		}

	}

	Ok (())

}

fn restore_extent_data <'a, FileType: Write + Seek> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	extent_data: & BtrfsExtentData,
	file_size_remaining: u64,
	file: & mut FileType,
	source: & Path,
	target: & Path,
) -> Result <u64, String> {

	match extent_data.extent_type () {

		BTRFS_EXTENT_DATA_INLINE_TYPE => {

			let inline_data =
				extent_data.inline_data ().map_err (
					|error|

					format! (
						"Error restoring {}: {}",
						target.to_string_lossy (),
						error)

				) ?.unwrap ();

			file.write_all (
				inline_data.as_ref (),
			).map_err (
				|error|

				format! (
					"Error writing data to {}",
					target.to_string_lossy ())

			) ?;

			Ok (inline_data.len () as u64)

		},

		BTRFS_EXTENT_DATA_REGULAR_TYPE => {

			if extent_data.extent_logical_address () != 0 {

				let raw_data =
					filesystem.slice_at_logical_address (
						extent_data.extent_logical_address (),
						extent_data.extent_size () as usize,
					) ?;

				let uncompressed_data =
					btrfs_decompress_pages (
						extent_data.compression (),
						raw_data,
						extent_data.extent_data_size (),
					) ?;

				let uncompressed_end_position =
					extent_data.extent_data_offset ()
						+ extent_data.extent_data_size ();

				if uncompressed_end_position
					> uncompressed_data.len () as u64 {

					return Err (
						"TODO".to_string ());
				}

				file.write_all (
					& uncompressed_data.as_ref () [
						extent_data.extent_data_offset () as usize
					..
						uncompressed_end_position as usize
					],
				).map_err (
					|error|

					format! (
						"Error writing data to {}: {}",
						target.to_string_lossy (),
						error.description ())

				) ?;

				Ok (extent_data.extent_data_size ())

			} else {

				// sparse extent

				file.seek (
					SeekFrom::Current (
						extent_data.extent_data_size () as i64),
				).map_err (
					|error|

					format! (
						"Error seeking past sparse extent in {}: {}",
						target.to_string_lossy (),
						error.description ())

				) ?;

				Ok (extent_data.extent_data_size ())

			}

		},

		BTRFS_EXTENT_DATA_PREALLOC_TYPE => {

			Ok (0)

		},

		_ => {

			Err (
				format! (
					"Invalid extent data type {} in {}",
					extent_data.extent_type (),
					target.to_string_lossy ()))

		}

	}

}


fn restore_directory <'a> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	object_id: u64,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	output_message! (
		output,
		"D {}",
		target.to_string_lossy ());

	let inode_item =
		filesystem_tree.inode_item (
			object_id,
		).ok_or (

			format! (
				"Error finding inode {}",
				object_id)

		) ?;

	let target_c =
		OsString::from_vec (
			target.as_os_str ().as_bytes ().to_vec ().into_iter ()
				.chain (b"\0".to_vec ().into_iter ())
				.collect ());

	// create the directory

	let result = unsafe {
		libc::mkdir (
			target_c.as_bytes ().as_ptr () as * const i8,
			0o0700)
	};

	if result != 0 {

		return Err (
			format! (
				"Error creating directory {}",
				target.to_string_lossy ()));

	}

	// set metadata

	restore_metadata (
		output,
		& inode_item,
		source,
		target,
		false,
	) ?;

	// recurse

	restore_children (
		output,
		filesystem,
		filesystem_tree,
		object_id,
		source,
		target,
	) ?;

	Ok (())

}

fn restore_metadata (
	output: & Output,
	inode_item: & BtrfsInodeItem,
	source: & Path,
	target: & Path,
	link: bool,
) -> Result <(), String> {

	let target_c =
		OsString::from_vec (
			target.as_os_str ().as_bytes ().to_vec ().into_iter ()
				.chain (b"\0".to_vec ().into_iter ())
				.collect ());

	// set ownership

	let result = unsafe {
		libc::lchown (
			target_c.as_bytes ().as_ptr () as * const i8,
			inode_item.st_uid (),
			inode_item.st_gid ())
	};

	if result != 0 {

		return Err (
			format! (
				"Error setting ownership on {}",
				target.to_string_lossy ()));

	}

	if ! link {

		// set mode

		let result = unsafe {
			libc::chmod (
				target_c.as_bytes ().as_ptr () as * const i8,
				inode_item.st_mode ())
		};

		if result != 0 {

			return Err (
				format! (
					"Error setting mode on {}",
					target.to_string_lossy ()));

		}

		// set times

		let result = unsafe {
			libc::utime (
				target_c.as_bytes ().as_ptr () as * const i8,
				& libc::utimbuf {
					actime: inode_item.st_atime ().seconds (),
					modtime: inode_item.st_mtime ().seconds (),
				},
			)
		};

		if result != 0 {

			return Err (
				format! (
					"Error setting times on {}",
					target.to_string_lossy ()));

		}

	}

	// return

	Ok (())

}

fn restore_symlink <'a> (
	output: & Output,
	filesystem: & 'a BtrfsFilesystem,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	object_id: u64,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	output_message! (
		output,
		"S {} (TODO)",
		target.to_string_lossy ());

	let inode_item =
		filesystem_tree.inode_item (
			object_id,
		).ok_or (

			format! (
				"Error finding inode {}",
				object_id)

		) ?;

	// restore to memory buffer

	let mut buffer: Vec <u8> =
		Vec::new ();

	let mut buffer_cursor =
		Cursor::new (
			buffer);

	restore_file_contents (
		output,
		filesystem,
		filesystem_tree,
		& inode_item,
		& mut buffer_cursor,
		source,
		target,
	) ?;

	let buffer =
		buffer_cursor.into_inner ();

	// create symlink

	unix_fs::symlink (
		Path::new (
			OsStr::from_bytes (
				& buffer)),
		target,
	).map_err (
		|error|

		format! (
			"Error creating symlink: {}",
			error.description ())

	) ?;

	// set metadata

	restore_metadata (
		output,
		& inode_item,
		source,
		target,
		true,
	) ?;

	// return

	Ok (())

}

// ex: noet ts=4 filetype=rust
