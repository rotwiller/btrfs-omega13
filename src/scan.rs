use std::collections::HashSet;

use output;
use output::OutputBox;

use arguments::*;
use filesystem::*;
use index::*;

pub fn scan (
	command: ScanCommand,
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

	// print data

	print_roots (
		& filesystem,
		& mut output);

	// return

	Ok (())

}

fn print_roots (
	filesystem: & Filesystem,
	output: & mut OutputBox,
) {

	// find parent dir entries

	let root_object_ids: HashSet <i64> =
		filesystem.dir_items_recent ().values ().filter (
			|&& (leaf_node_header, _dir_item)|

			! filesystem.dir_items_recent ().contains_key (
				& leaf_node_header.key.object_id)

		).map (
			|& (leaf_node_header, _dir_item)|

			leaf_node_header.key.object_id

		).collect ();

	// print information about roots

	for root_object_id in root_object_ids {

		output.message (
			& format! (
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
	output: & mut OutputBox,
	indent: & str,
	object_id: i64,
) {

	if let Some (child_object_ids) =
		filesystem.dir_items_by_parent ().get (
			& object_id) {

		let next_indent =
			format! (
				"{}  ",
				indent);

		for child_object_id in child_object_ids {

			let & (_child_leaf_node_header, child_dir_item) =
				filesystem.dir_items_recent ().get (
					child_object_id,
				).unwrap ();

			output.message (
				& format! (
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
