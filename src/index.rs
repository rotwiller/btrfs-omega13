use std::error::Error;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::fs::File;
use std::mem;
use std::path::Path;

use btrfs::diskformat::*;

use memmap::Mmap;
use memmap::Protection;

use output;
use output::OutputBox;

use uuid::Uuid;

use arguments::*;

pub fn index (
	command: IndexCommand,
) -> Result <(), String> {

	let mut output =
		output::open ();

	let mut node_positions: Vec <usize> =
		Vec::new ();

	let mut offset: usize = 0;

	for path in command.paths.iter () {

		try! (
			index_scan (
				& mut output,
				& mut node_positions,
				& path,
				& mut offset));

	}

	let mut index_file = try! (
		File::create (
			& command.index,
		).map_err (
			|error|

			format! (
				"Error creating {}: {}",
				command.index.to_string_lossy (),
				error.description ())
		)
	);

	try! (
		index_write (
			& node_positions,
			& mut index_file));

	Ok (())

}

fn index_scan (
	output: & mut OutputBox,
	node_positions: & mut Vec <usize>,
	path: & Path,
	offset: & mut usize,
) -> Result <(), String> {

	// mmap target

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

	output.message (
		& format! (
			"Scanning {}",
			path.to_string_lossy ()));

	// read superblock

	let superblock: & BtrfsSuperblock = unsafe {
		& * (
			mmap.ptr ().offset (0x1_0000)
			as * const BtrfsSuperblock
		)
	};

	if superblock.magic != BTRFS_MAGIC {

		return Err (
			"Superblock not found".to_owned ());

	}

	// print fs information

	output.message (
		& format! (
			"Filesystem UUID: {}",
			Uuid::from_bytes (
				& superblock.fs_uuid,
			).unwrap ()));

	output.message (
		& format! (
			"Node size: 0x{:x}",
			superblock.node_size));

	output.message (
		& format! (
			"Leaf size: 0x{:x}",
			superblock.leaf_size));

	if superblock.node_size != superblock.leaf_size {

		panic! (
			"TODO - handle node size and leaf size different");

	}

	// scan for nodes

	let mut position: usize =
		0x1_1000;

	let max_position: usize =
		mmap.len () - mmap.len () % superblock.node_size as usize;

	let status_size: usize =
		superblock.sector_size as usize * 0x1000;

	output.message (
		& format! (
			"Scanning from 0x{:x} to 0x{:x}",
			position,
			max_position));

	while position < max_position {

		if position % status_size == 0 {

			output.status (
				& format! (
					"At position 0x{:x} ({}%)",
					position,
					position * 100 / mmap.len ()));

		}

		// skip superblocks

		if (
			position == 0x400_0000
			|| position == 0x40_0000_0000
			|| position == 0x4_0000_0000_0000
		) {

			position +=
				superblock.sector_size as usize;

			continue;

		}

		// check for header

		let node_header: & BtrfsNodeHeader = unsafe {
			& * (
				mmap.ptr ().offset (position as isize)
				as * const BtrfsNodeHeader
			)
		};

		if node_header.fs_uuid != superblock.fs_uuid {

			position +=
				superblock.sector_size as usize;

			continue;

		}

		// store it

		output.message (
			& format! (
				"Found node at 0x{:x} in tree {}",
				position,
				node_header.tree_id));

		node_positions.push (
			* offset + position);

		// continue

		position +=
			superblock.sector_size as usize;

	}

	// return

	output.clear_status ();

	* offset += mmap.len ();

	Ok (())

}

fn index_write (
	node_positions: & Vec <usize>,
	index_writer: & mut Write,
) -> Result <(), String> {

	let mut index_writer =
		BufWriter::new (
			index_writer);

	for position in node_positions.iter () {

		try! (
			index_writer.write_all (
				format! (
					"{:x}\n",
					position,
				).as_bytes (),
			).map_err (
				|error| error.description ().to_owned (),
			)
		);

	}

	Ok (())

}

pub fn index_load (
	index_path: & Path,
) -> Result <Vec <usize>, String> {

	let mut index_file = try! (
		File::open (
			index_path,
		).map_err (
			|error|

			format! (
				"Error opening {}: {}",
				index_path.to_string_lossy (),
				error.description ())

		)
	);

	let mut node_positions: Vec <usize> =
		Vec::new ();

	try! (
		index_read (
			& mut node_positions,
			& mut index_file,
		).map_err (
			|error|

			format! (
				"Error reading {}: {}",
				index_path.to_string_lossy (),
				error)

		)
	);

	Ok (node_positions)

}

fn index_read (
	node_positions: & mut Vec <usize>,
	index_reader: & mut Read,
) -> Result <(), String> {

	let index_reader =
		BufReader::new (
			index_reader);

	for line in index_reader.lines () {

		let line = try! (
			line.map_err (
				|error| error.description ().to_owned ()
			)
		);

		node_positions.push (
			usize::from_str_radix (
				& line,
				16,
			).unwrap ());

	}

	Ok (())

}

// ex: noet ts=4 filetype=rust
