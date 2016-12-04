use std::collections::HashSet;
use std::ffi::OsString;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;

use btrfs::diskformat::*;

use libc;

use output;
use output::OutputBox;

use arguments::*;
use filesystem::*;
use index::*;

pub fn restore (
	command: RestoreCommand,
) -> Result <(), String> {

	let mut output =
		output::open ();

	let (node_positions, mmaps) =
		try! (
			load_index_and_mmaps (
				& mut output,
				& command.index,
				& command.paths));

	// reconstruct file system

	let mut filesystem =
		Filesystem::new (
			& node_positions,
			& mmaps);

	filesystem.build_main_index (
		& mut output);

	filesystem.build_dir_items_index (
		& mut output);

	filesystem.build_inode_items_index (
		& mut output);

	// restore files

	restore_children (
		& mut output,
		& filesystem,
		command.object_id,
		& command.target);

	// return

	Ok (())

}

fn restore_children (
	output: & mut OutputBox,
	filesystem: & Filesystem,
	object_id: i64,
	target: & Path,
) {

	output.status (
		"Restoring files ...");

	// iterate children

	if let Some (child_object_ids) =
		filesystem.dir_items_by_parent ().get (
			& object_id) {

		for & child_object_id in child_object_ids {

			restore_dir_item (
				output,
				filesystem,
				child_object_id,
				target);

		}

	}

}

fn restore_dir_item (
	output: & mut OutputBox,
	filesystem: & Filesystem,
	object_id: i64,
	target: & Path,
) {

	let & (_dir_item_leaf, dir_item) =
		filesystem.dir_items_recent ().get (
			& object_id,
		).unwrap ();

	let target =
		target.join (
			OsString::from_vec (
				dir_item.name ().to_vec ()));

	let target_c =
		OsString::from_vec (
			target.as_os_str ().as_bytes ().to_vec ().into_iter ()
				.chain (b"\0".to_vec ().into_iter ())
				.collect ());

	if let Some (& (_inode_item_leaf, inode_item)) =
		filesystem.inode_items_recent ().get (
			& object_id) {

		match dir_item.child_type {

			BTRFS_CHILD_REGULAR_FILE_TYPE => {

				// TODO

			},

			BTRFS_CHILD_DIRECTORY_TYPE => {

				let result = unsafe {
					libc::mkdir (
						target_c.as_bytes ().as_ptr () as * const i8,
						0o0700)
				};

				if result != 0 {

					output.message (
						& format! (
							"Error creating directory {}",
							target.to_string_lossy ()));

					return;

				}

				restore_children (
					output,
					filesystem,
					object_id,
					& target);

				let result = unsafe {
					libc::chmod (
						target_c.as_bytes ().as_ptr () as * const i8,
						inode_item.st_mode)
				};

				if result != 0 {

					output.message (
						& format! (
							"Error setting mode on directory {}",
							target.to_string_lossy ()));

					return;

				}

				let result = unsafe {
					libc::chown (
						target_c.as_bytes ().as_ptr () as * const i8,
						inode_item.st_uid,
						inode_item.st_gid)
				};

				if result != 0 {

					output.message (
						& format! (
							"Error setting ownership on directory {}",
							target.to_string_lossy ()));

					return;

				}

			},

			BTRFS_CHILD_SYMBOLIC_LINK_TYPE => {

				// TODO

			},

			_ => {

				output.message (
					& format! (
						"Unknown dir item type {}",
						dir_item.child_type));

			},

		}

	} else {

		output.message (
			& format! (
				"Unable to get inode {} for {}",
				object_id,
				target.to_string_lossy ()));

	}

}

// ex: noet ts=4 filetype=rust
