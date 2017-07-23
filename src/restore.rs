use std::collections::HashSet;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;

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

	let mut object_id =
		root_item.root_object_id ();

	for path_part in command.source.iter () {

		if path_part == "/" {
			continue;
		}

		let dir_item_entry =
			filesystem_tree.dir_item_entry (
				object_id,
				path_part.as_bytes (),
			).ok_or (

				format! (
					"Path not found: {}",
					command.source.to_string_lossy ())

			) ?;

		object_id =
			dir_item_entry.child_object_id ();

	}

	let inode_item =
		filesystem_tree.inode_item (
			object_id,
		).ok_or (

			format! (
				"Inode item not found: {}",
				object_id)

		) ?;

	let output_job =
		output_job_start! (
			output,
			"Restoring files");

	restore_directory (
		output,
		& filesystem_tree,
		object_id,
		& command.source,
		& command.target,
	) ?;

	output_job.complete ();

	// return

	Ok (())

}

fn restore_item <'a> (
	output: & Output,
	filesystem_tree: & 'a BtrfsFilesystemTree <'a>,
	dir_index: & 'a BtrfsDirIndex <'a>,
	source: & Path,
	target: & Path,
) -> Result <(), String> {

	match dir_index.child_type () {

		BTRFS_FT_REG_FILE =>
			restore_regular_file (
				output,
				filesystem_tree,
				dir_index.child_object_id (),
				& source,
				& target,
			) ?,

		BTRFS_FT_DIR =>
			restore_directory (
				output,
				filesystem_tree,
				dir_index.child_object_id (),
				& source,
				& target,
			) ?,

		BTRFS_FT_SYMLINK =>
			output_message! (
				output,
				"TODO: symlink {}",
				source.to_string_lossy ()),

		_ =>
			output_message! (
				output,
				"Unknown dir item entry type {}: {}",
				dir_index.child_type (),
				source.to_string_lossy ()),

	}

	Ok (())

}

fn restore_children <'a> (
	output: & Output,
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

		restore_item (
			output,
			filesystem_tree,
			& dir_index,
			& source,
			& target,
		) ?;

	}

	Ok (())

}

fn restore_regular_file <'a> (
	output: & Output,
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

	// find contents

	/*
	if let Some (extent_datas) =
		indexed_filesystem.extent_datas_index ().get (
			& object_id) {

		let mut file_position: u64 = 0;

		for extent_data in extent_datas {

			output.message_format (
				format_args! (
					"file position: {}",
					file_position));

			if extent_data.offset () != file_position {

				output.message_format (
					format_args! (
						"Extents not in order for {}: expected {}, but got {}",
						target.to_string_lossy (),
						file_position,
						extent_data.offset ()));

				return;

			}

			restore_extent_data (
				output,
				indexed_filesystem,
				target,
				extent_data,
				& mut file);

			file_position +=
				extent_data.logical_data_size ();

		}

	}
	*/

	// set metadata

	restore_metadata (
		output,
		& inode_item,
		source,
		target,
	) ?;

	// return

	Ok (())

}

/*
fn restore_extent_data (
	output: & Output,
	indexed_filesystem: & IndexedFilesystem,
	target: & Path,
	extent_data: & BtrfsExtentData,
	file: & mut File,
) {

	match extent_data.extent_type () {

		BTRFS_EXTENT_DATA_INLINE_TYPE => {

			let inline_data = match (
				extent_data.inline_data ()
			) {

				Ok (Some (data)) =>
					data,

				Ok (None) =>
					panic! (),

				Err (error) => {

					output.message_format (
						format_args! (
							"Error restoring {}: {}",
							target.to_string_lossy (),
							error));

					return;

				},

			};

			if let Err (error) = (
				file.write_all (
					inline_data.as_ref ())
			) {

				output.message_format (
					format_args! (
						"Error writing data to {}",
						target.to_string_lossy ()));

				return;

			}

		},

		BTRFS_EXTENT_DATA_REGULAR_TYPE => {

			let extent_items: Option <& Vec <BtrfsExtentItem>> =
				indexed_filesystem.extent_items_index ().get (
					& extent_data.logical_address ());

			if extent_items.is_none () {

				output.message_format (
					format_args! (
						"Missing extent item {} for {}",
						extent_data.logical_address (),
						target.to_string_lossy ()));

				return;

			}

			let extent_items =
				extent_items.unwrap ();

			let extent_items: HashSet <& BtrfsExtentItem> =
				extent_items.into_iter ().filter (
					|&& extent_item|

					extent_item.offset () as u64
						== extent_data.extent_size ()

				).collect ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().collect ();

			if extent_items.is_empty () {

				output.message_format (
					format_args! (
						"Missing extent item {} for {}",
						extent_data.logical_address (),
						target.to_string_lossy ()));

				return;

			}

			let max_generation =
				extent_items.iter ().map (
					|& extent_item|

					extent_item.generation ()

				).max ().unwrap ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().filter (
					|& extent_item|

					extent_item.generation () == max_generation

				).collect ();

			let max_reference_count =
				extent_items.iter ().map (
					|& extent_item|

					extent_item.reference_count ()

				).max ().unwrap ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().filter (
					|& extent_item|

					extent_item.reference_count () == max_reference_count

				).collect ();

			if extent_items.len () > 1 {

				output.message_format (
					format_args! (
						"Multiple extent items {} for {}",
						extent_data.logical_address (),
						target.to_string_lossy ()));

				for & extent_item
				in extent_items.iter () {

					output.message_format (
						format_args! (
							"  Generation {} ref count {} flags {} first \
							entry key {:?} level {}",
							extent_item.generation (),
							extent_item.reference_count (),
							extent_item.flags (),
							extent_item.first_entry_key (),
							extent_item.level ()));

				}

				return;

			}

			// TODO write extent data

		},

		BTRFS_EXTENT_DATA_PREALLOC_TYPE => {

			// TODO ignore?

		},

		_ => {

			output.message_format (
				format_args! (
					"Invalid extent data type {} in {}",
					extent_data.extent_type (),
					target.to_string_lossy ()));

		}

	}

}
*/

fn restore_directory <'a> (
	output: & Output,
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

	restore_children (
		output,
		filesystem_tree,
		object_id,
		source,
		target,
	) ?;

	restore_metadata (
		output,
		& inode_item,
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
) -> Result <(), String> {

	let target_c =
		OsString::from_vec (
			target.as_os_str ().as_bytes ().to_vec ().into_iter ()
				.chain (b"\0".to_vec ().into_iter ())
				.collect ());

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

	// set ownership

	let result = unsafe {
		libc::chown (
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

	// TODO times, etc

	Ok (())

}

// ex: noet ts=4 filetype=rust
