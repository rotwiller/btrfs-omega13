#![ allow (unused_parens) ]

#![ deny (non_camel_case_types) ]
#![ deny (non_snake_case) ]
#![ deny (non_upper_case_globals) ]
#![ deny (unreachable_patterns) ]
#![ deny (unused_comparisons) ]
#![ deny (unused_must_use) ]

extern crate btrfs;
extern crate clap;
extern crate crc;
extern crate libc;
extern crate memmap;
extern crate uuid;

#[ macro_use ]
extern crate output;

mod arguments;
mod indexed_filesystem;
mod index;
mod restore;
mod scan;

use std::error::Error;
use std::panic;
use std::process;

use output::*;

use arguments::*;
use index::*;
use restore::*;
use scan::*;

fn main () {

	let output =
		output::open ();

//	let output =
//		output::open ().enable_debug ();

	match main_real (
		& output,
	) {

		Ok (_) =>
			process::exit (0),

		Err (error) => {

			output_message! (
				output,
				"{}",
				error);

			process::exit (1);

		}

	}

}

fn main_real (
	output: & Output,
) -> Result <(), String> {

	match panic::catch_unwind (|| {

		if let Some (command) =
			parse_arguments () {

			match command {

				Command::Index (index_command) =>
					index (
						& output,
						index_command),

				Command::Restore (restore_command) =>
					restore (
						& output,
						restore_command),

				Command::Scan (scan_command) =>
					scan (
						& output,
						scan_command),

			}

		} else {

			Ok (())

		}

	}) {

		Ok (result) =>
			result,

		Err (panic) => {

			if let Some (ref string) =
				panic.downcast_ref::<String> () {

				Err (string.to_string ())

			} else if let Some (error) =
				panic.downcast_ref::<Box <Error>> () {

				Err (error.description ().to_string ())

			} else {

				Err ("Unknown error".to_string ())

			}

		},

	}

}

// ex: noet ts=4 filetype=rust
