use std::collections::HashSet;

use btrfs::diskformat::*;

use output;
use output::Output;

use arguments::*;
use device_maps::*;
use filesystem::*;

pub fn scan (
	command: ScanCommand,
) -> Result <(), String> {

	let mut output =
		output::open ();

	// open devices

	let device_maps =
		DeviceMaps::open (
			& command.paths,
		) ?;

	let mut btrfs_device_map =
		BtrfsDeviceMap::new ();

	btrfs_device_map.insert (
		1,
		device_maps.get_data ().into_iter ().next ().unwrap ());

	// load filesystem

	let filesystem =
		Filesystem::load_with_index (
			& mut output,
			& command.index,
			& btrfs_device_map,
		) ?;

	// print data

	print_roots (
		& filesystem,
		& mut output);

	// return

	Ok (())

}

fn print_roots (
	filesystem: & Filesystem,
	output: & Output,
) {

	// find parent dir entries

	let root_object_ids: HashSet <u64> =
		filesystem.dir_items_recent ().values ().filter (
			|&& dir_item|

			! filesystem.dir_items_recent ().contains_key (
				& dir_item.object_id ())

		).map (
			|& dir_item|

			dir_item.object_id ()

		).collect ();

	// print information about roots

	for root_object_id in root_object_ids {

		output.message_format (
			format_args! (
				"ROOT: {}",
				root_object_id));

		print_tree (
			filesystem,
			output,
			"  ",
			root_object_id);

	}

}

fn print_tree (
	filesystem: & Filesystem,
	output: & Output,
	indent: & str,
	object_id: u64,
) {

	if let Some (child_object_ids) =
		filesystem.dir_items_by_parent ().get (
			& object_id) {

		let next_indent =
			format! (
				"{}  ",
				indent);

		for child_object_id in child_object_ids {

			let child_dir_item =
				filesystem.dir_items_recent ().get (
					child_object_id,
				).unwrap ();

			output.message_format (
				format_args! (
					"{}{}",
					indent,
					String::from_utf8_lossy (
						child_dir_item.name ())));

			print_tree (
				filesystem,
				output,
				& next_indent,
				* child_object_id);

		}

	}

}

// ex: noet ts=4 filetype=rust
