use std::collections::HashSet;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
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

	let & (dir_item_leaf, dir_item) =
		filesystem.dir_items_recent ().get (
			& object_id,
		).unwrap ();

	let target =
		target.join (
			OsString::from_vec (
				dir_item.name ().to_vec ()));

	if let Some (& (inode_item_leaf, inode_item)) =
		filesystem.inode_items_recent ().get (
			& object_id) {

		match dir_item.child_type {

			BTRFS_CHILD_REGULAR_FILE_TYPE =>
				restore_regular_file (
					output,
					filesystem,
					object_id,
					& target,
					dir_item_leaf,
					dir_item,
					inode_item_leaf,
					inode_item),

			BTRFS_CHILD_DIRECTORY_TYPE =>
				restore_directory (
					output,
					filesystem,
					object_id,
					& target,
					dir_item_leaf,
					dir_item,
					inode_item_leaf,
					inode_item),

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

fn restore_regular_file (
	output: & mut OutputBox,
	filesystem: & Filesystem,
	object_id: i64,
	target: & Path,
	dir_item_leaf: & BtrfsLeafNodeHeader,
	dir_item: & BtrfsDirItem,
	inode_item_leaf: & BtrfsLeafNodeHeader,
	inode_item: & BtrfsInodeItem,
) {

	// create file

	let mut file = match (
		File::create (
			target)
	) {

		Ok (file) => file,

		Err (error) => {

			output.message (
				& format! (
					"Error creating file {}",
					error.description ()));

			return;

		}

	};

	// find contents

	if let Some (extent_datas) =
		filesystem.extent_datas_index ().get (
			& object_id) {

		for & (extent_data_leaf, extent_data) in extent_datas {

			restore_extent_data (
				output,
				filesystem,
				target,
				extent_data_leaf,
				extent_data,
				& mut file);

		}

	}

	// set metadata

	restore_metadata (
		output,
		target,
		inode_item);

}

fn restore_extent_data (
	output: & mut OutputBox,
	filesystem: & Filesystem,
	target: & Path,
	extent_data_leaf: & BtrfsLeafNodeHeader,
	extent_data: & BtrfsExtentData,
	file: & mut File,
) {

	// TODO seek

	match extent_data.extent_type {

		BTRFS_EXTENT_DATA_INLINE_TYPE => {

			// TODO write extent data

		},

		BTRFS_EXTENT_DATA_REGULAR_TYPE => {

			let extent_items =
				filesystem.extent_items_index ().get (
					& extent_data.logical_address);

			if extent_items.is_none () {

				output.message (
					& format! (
						"Missing extent item {} for {}",
						extent_data.logical_address as i64,
						target.to_string_lossy ()));

				return;

			}

			let extent_items =
				extent_items.unwrap ();

			let extent_items: HashSet <& BtrfsExtentItem> =
				extent_items.iter ().filter (
					|&& (extent_item_leaf, extent_item)|

					extent_item_leaf.key.offset as u64
						== extent_data.extent_size

				).map (
					|& (extent_item_leaf, extent_item)|

					extent_item

				).collect ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().collect ();

			if extent_items.is_empty () {

				output.message (
					& format! (
						"Missing extent item {} for {}",
						extent_data.logical_address as i64,
						target.to_string_lossy ()));

				return;

			}

			let max_generation =
				extent_items.iter ().map (
					|& extent_item|

					extent_item.generation

				).max ().unwrap ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().filter (
					|& extent_item|

					extent_item.generation == max_generation

				).collect ();

			let max_reference_count =
				extent_items.iter ().map (
					|& extent_item|

					extent_item.reference_count

				).max ().unwrap ();

			let extent_items: Vec <& BtrfsExtentItem> =
				extent_items.into_iter ().filter (
					|& extent_item|

					extent_item.reference_count == max_reference_count

				).collect ();

			if extent_items.len () > 1 {

				output.message (
					& format! (
						"Multiple extent items {} for {}",
						extent_data.logical_address as i64,
						target.to_string_lossy ()));

				for & extent_item
				in extent_items.iter () {

					output.message (
						& format! (
							"  Generation {} ref count {} flags {} first \
							entry key {:?} level {}",
							extent_item.generation,
							extent_item.reference_count,
							extent_item.flags,
							extent_item.first_entry_key,
							extent_item.level));

				}

				return;

			}

			// TODO write extent data

		},

		BTRFS_EXTENT_DATA_PREALLOC_TYPE => {

			// TODO ignore?

		},

		_ => {

			output.message (
				& format! (
					"Invalid extent data type {} in {}",
					extent_data.extent_type,
					target.to_string_lossy ()));

		}

	}

}

fn restore_directory (
	output: & mut OutputBox,
	filesystem: & Filesystem,
	object_id: i64,
	target: & Path,
	dir_item_leaf: & BtrfsLeafNodeHeader,
	dir_item: & BtrfsDirItem,
	inode_item_leaf: & BtrfsLeafNodeHeader,
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

	restore_metadata (
		output,
		target,
		inode_item);

}

fn restore_metadata (
	output: & mut OutputBox,
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
			inode_item.st_mode)
	};

	if result != 0 {

		output.message (
			& format! (
				"Error setting mode on {}",
				target.to_string_lossy ()));

		return;

	}

	// set ownership

	let result = unsafe {
		libc::chown (
			target_c.as_bytes ().as_ptr () as * const i8,
			inode_item.st_uid,
			inode_item.st_gid)
	};

	if result != 0 {

		output.message (
			& format! (
				"Error setting ownership on {}",
				target.to_string_lossy ()));

		return;

	}

}

// ex: noet ts=4 filetype=rust
