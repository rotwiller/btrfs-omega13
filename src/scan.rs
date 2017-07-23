use std::collections::HashSet;

use btrfs::diskformat::*;

use output::Output;

use super::arguments::*;
use super::indexed_filesystem::*;

pub fn scan (
	output: & Output,
	command: ScanCommand,
) -> Result <(), String> {

	// open filesystem

	let mmap_devices =
		BtrfsMmapDeviceSet::open (
			& command.paths,
		) ?;

	let devices =
		mmap_devices.devices () ?;

	let filesystem =
		BtrfsFilesystem::open_try_backups (
			output,
			& devices,
		) ?;

	// print out subvolumes

	let default_subvolume_root_item =
		filesystem.default_subvolume_root_item ().ok_or (
			"No default subvolume root item"
		) ?;

	output_message! (
		output,
		"Subvolumes:");

	output_message! (
		output,
		"  ROOT (5)");

	for root_backref in filesystem.subvolume_root_backrefs () {

		let path =
			filesystem.subvolume_path (
				root_backref,
			) ?;

		output_message! (
			output,
			"  {} ({})",
			path.to_string_lossy (),
			root_backref.object_id ());

	}

	// return

	Ok (())

}

fn print_roots (
	indexed_filesystem: & IndexedFilesystem,
	output: & Output,
) {

	// find parent dir entries

	let root_object_ids: HashSet <u64> =
		indexed_filesystem.dir_item_entries_recent ().values ().filter (
			|&& dir_item_entry|

			! indexed_filesystem.dir_item_entries_recent ().contains_key (
				& dir_item_entry.object_id ())

		).map (
			|& dir_item_entry|

			dir_item_entry.object_id ()

		).collect ();

	// print information about roots

	for root_object_id in root_object_ids {

		output.message_format (
			format_args! (
				"ROOT: {}",
				root_object_id));

		print_tree (
			indexed_filesystem,
			output,
			"  ",
			root_object_id,
			2);

	}

}

fn print_tree (
	indexed_filesystem: & IndexedFilesystem,
	output: & Output,
	indent: & str,
	object_id: u64,
	max_depth: u64,
) {

	if let Some (child_object_ids) =
		indexed_filesystem.dir_item_entries_by_parent ().get (
			& object_id) {

		let next_indent =
			format! (
				"{}  ",
				indent);

		for child_object_id in child_object_ids {

			let child_dir_item_entry =
				indexed_filesystem.dir_item_entries_recent ().get (
					child_object_id,
				).unwrap ();

			output.message_format (
				format_args! (
					"{}{}",
					indent,
					String::from_utf8_lossy (
						child_dir_item_entry.name ())));

			if max_depth > 0 {

				print_tree (
					indexed_filesystem,
					output,
					& next_indent,
					* child_object_id,
					max_depth - 1);

			}

		}

	}

}

// ex: noet ts=4 filetype=rust
