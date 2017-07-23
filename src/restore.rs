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

	// open devices

	let mmap_devices =
		BtrfsMmapDeviceSet::open (
			& command.paths,
		) ?;

	let mut devices =
		mmap_devices.devices () ?;

	// load filesystem

	let filesystem =
		BtrfsFilesystem::open_try_backups (
			output,
			& devices,
		) ?;

	let indexed_filesystem =
		IndexedFilesystem::open (
			output,
			& filesystem,
			& command.index,
		) ?;

	// restore files

	restore_children (
		output,
		& indexed_filesystem,
		command.object_id as u64,
		& command.target);

	// return

	Ok (())

}

fn restore_children (
	output: & Output,
	indexed_filesystem: & IndexedFilesystem,
	object_id: u64,
	target: & Path,
) {

	let output_job =
		output_job_start! (
			output,
			"Restoring files ...");

	// iterate children

	if let Some (child_object_ids) =
		indexed_filesystem.dir_item_entries_by_parent ().get (
			& object_id) {

		for & child_object_id in child_object_ids {

			restore_dir_item (
				output,
				indexed_filesystem,
				child_object_id,
				target);

		}

	}

	output_job.complete ();

}

fn restore_dir_item (
	output: & Output,
	indexed_filesystem: & IndexedFilesystem,
	object_id: u64,
	target: & Path,
) {

	let dir_item_entry =
		indexed_filesystem.dir_item_entries_recent ().get (
			& object_id,
		).unwrap ();

	let target =
		target.join (
			OsString::from_vec (
				dir_item_entry.name ().to_vec ()));

	if let Some (inode_item) =
		indexed_filesystem.inode_items_recent ().get (
			& object_id) {

		match dir_item_entry.child_type () {

			BTRFS_CHILD_REGULAR_FILE_TYPE =>
				restore_regular_file (
					output,
					indexed_filesystem,
					object_id,
					& target,
					dir_item_entry,
					inode_item),

			BTRFS_CHILD_DIRECTORY_TYPE =>
				restore_directory (
					output,
					indexed_filesystem,
					object_id,
					& target,
					dir_item_entry,
					inode_item),

			BTRFS_CHILD_SYMBOLIC_LINK_TYPE => {

				// TODO

			},

			_ => {

				output.message_format (
					format_args! (
						"Unknown dir item entry type {}",
						dir_item_entry.child_type ()));

			},

		}

	} else {

		output.message_format (
			format_args! (
				"Unable to get inode {} for {}",
				object_id,
				target.to_string_lossy ()));

	}

}

fn restore_regular_file (
	output: & Output,
	indexed_filesystem: & IndexedFilesystem,
	object_id: u64,
	target: & Path,
	dir_item_entry: & BtrfsDirItemEntry,
	inode_item: & BtrfsInodeItem,
) {

	// create file

	let mut file = match (
		File::create (
			target)
	) {

		Ok (file) => file,

		Err (error) => {

			output.message_format (
				format_args! (
					"Error creating {}: {}",
					target.to_string_lossy (),
					error.description ()));

			return;

		}

	};

	// find contents

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

	// set metadata

	restore_metadata (
		output,
		target,
		inode_item);

}

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

fn restore_directory (
	output: & Output,
	indexed_filesystem: & IndexedFilesystem,
	object_id: u64,
	target: & Path,
	dir_item_entry: & BtrfsDirItemEntry,
	inode_item: & BtrfsInodeItem,
) {

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

		output.message_format (
			format_args! (
				"Error creating directory {}",
				target.to_string_lossy ()));

		return;

	}

	restore_children (
		output,
		indexed_filesystem,
		object_id,
		& target);

	restore_metadata (
		output,
		target,
		inode_item);

}

fn restore_metadata (
	output: & Output,
	target: & Path,
	inode_item: & BtrfsInodeItem,
) {

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

		output.message_format (
			format_args! (
				"Error setting mode on {}",
				target.to_string_lossy ()));

		return;

	}

	// set ownership

	let result = unsafe {
		libc::chown (
			target_c.as_bytes ().as_ptr () as * const i8,
			inode_item.st_uid (),
			inode_item.st_gid ())
	};

	if result != 0 {

		output.message_format (
			format_args! (
				"Error setting ownership on {}",
				target.to_string_lossy ()));

		return;

	}

	// TODO times, etc

}

// ex: noet ts=4 filetype=rust
