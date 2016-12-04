#![ allow (unused_parens) ]

extern crate btrfs;
extern crate clap;
extern crate libc;
extern crate memmap;
extern crate output;
extern crate uuid;

mod arguments;
mod filesystem;
mod index;
mod restore;
mod scan;

use std::process;

use arguments::*;
use index::*;
use restore::*;
use scan::*;

fn main () {

	match main_real () {

		Ok (_) =>
			process::exit (0),

		Err (error) => {

			println! (
				"{}",
				error);

			process::exit (1);

		}

	}

}

fn main_real (
) -> Result <(), String> {

	if let Some (command) =
		parse_arguments () {

		match command {

			Command::Index (index_command) =>
				index (index_command),

			Command::Restore (restore_command) =>
				restore (restore_command),

			Command::Scan (scan_command) =>
				scan (scan_command),

		}

	} else {

		Ok (())

	}

}

// ex: noet ts=4 filetype=rust
