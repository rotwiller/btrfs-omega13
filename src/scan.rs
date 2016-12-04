use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::mem;
use std::rc::Rc;

use btrfs::diskformat::*;

use memmap::Mmap;
use memmap::Protection;

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

	// load index

	output.status (
		& format! (
			"Loading index from {} ...",
			command.index.to_string_lossy ()));

	let node_positions =
		try! (
			index_load (
				& command.index));

	output.clear_status ();

	output.message (
		& format! (
			"Loading index from {} ... done",
			command.index.to_string_lossy ()));

	// open devices

	let mut mmaps: Vec <Mmap> =
		Vec::new ();

	for path in command.paths.iter () {

		let file = try! (
			File::open (
				path,
			).map_err (
				|error|

				format! (
					"Error opening {}: {}",
					path.to_string_lossy (),
					error.description ())

			)
		);

		let mmap = try! (
			Mmap::open (
				& file,
				Protection::Read,
			).map_err (
				|error|

				format! (
					"Error mmaping {}: {}",
					path.to_string_lossy (),
					error.description ())

			)
		);

		mmaps.push (
			mmap);

	}

	// reconstruct file system

	let mut filesystem =
		Filesystem::new (
			& node_positions,
			& mmaps);

	filesystem.index (
		& mut output);

	filesystem.print_roots (
		& mut output);

	// return

	Ok (())

}

// ex: noet ts=4 filetype=rust
