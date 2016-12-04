use std::path::PathBuf;

use clap;

pub struct IndexCommand {
	pub paths: Vec <PathBuf>,
	pub index: PathBuf,
}

pub struct ScanCommand {
	pub paths: Vec <PathBuf>,
	pub index: PathBuf,
}

pub enum Command {
	Index (IndexCommand),
	Scan (ScanCommand),
}

pub fn parse_arguments (
) -> Option <Command> {

	let matches =
		application ().get_matches ();

	if let Some (index_matches) = (
		matches.subcommand_matches (
			"index")
	) {
		return Some (
			index_command (
				index_matches)
		);
	}

	if let Some (scan_matches) = (
		matches.subcommand_matches (
			"scan")
	) {
		return Some (
			scan_command (
				scan_matches)
		);
	}

	None

}

fn index_command (
	index_matches: & clap::ArgMatches,
) -> Command {

	let index =
		PathBuf::from (
			index_matches.value_of_os (
				"index",
			).unwrap ());

	let paths =
		index_matches.values_of_os (
			"path",
		).unwrap ().map (
			|os_value|

			PathBuf::from (
				os_value)

		).collect ();

	Command::Index (
		IndexCommand {
			paths: paths,
			index: index,
		}
	)

}

fn scan_command (
	scan_matches: & clap::ArgMatches,
) -> Command {

	let index =
		PathBuf::from (
			scan_matches.value_of_os (
				"index",
			).unwrap ());

	let paths =
		scan_matches.values_of_os (
			"path",
		).unwrap ().map (
			|os_value|

			PathBuf::from (
				os_value)

		).collect ();

	Command::Scan (
		ScanCommand {
			paths: paths,
			index: index,
		}
	)

}

fn application <'a, 'b> (
) -> clap::App <'a, 'b> {

	clap::App::new (
		"Btrfs Omega13")

		.about (
			"Low-level recovery tool for BTRFS file systems")

		.subcommand (
			clap::SubCommand::with_name (
				"index")

			.about (
				"builds an index of btrfs nodes")

			.arg (
				index_argument ())

			.arg (
				path_argument ())

		)

		.subcommand (
			clap::SubCommand::with_name (
				"scan")

			.about (
				"scans for files using an index")

			.arg (
				index_argument ())

			.arg (
				path_argument ())

		)

}

fn path_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("path")

		.value_name ("PATH")
		.required (true)
		.multiple (true)
		.index (1)

		.help (
			"Path to the BTRFS image(s) to recover")


}

fn index_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("index")

		.long ("index")
		.value_name ("FILE")
		.required (true)

		.help (
			"Index file")


}

// ex: noet ts=4 filetype=rust
