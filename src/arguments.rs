use std::path::PathBuf;

use clap;

pub struct IndexCommand {
	pub paths: Vec <PathBuf>,
	pub index: PathBuf,
}

pub struct ScanCommand {
	pub paths: Vec <PathBuf>,
}

pub struct RestoreCommand {
	pub paths: Vec <PathBuf>,
	pub subvolume_id: u64,
	pub source: PathBuf,
	pub target: PathBuf,
}

pub enum Command {
	Index (IndexCommand),
	Scan (ScanCommand),
	Restore (RestoreCommand),
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

	if let Some (restore_matches) = (
		matches.subcommand_matches (
			"restore")
	) {
		return Some (
			restore_command (
				restore_matches)
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

fn restore_command (
	restore_matches: & clap::ArgMatches,
) -> Command {

	let paths =
		restore_matches.values_of_os (
			"path",
		).unwrap ().map (
			|os_value|

			PathBuf::from (
				os_value)

		).collect ();

	let subvolume_id =
		u64::from_str_radix (
			restore_matches.value_of (
				"subvolume-id",
			).unwrap (),
			10,
		).unwrap ();

	let source =
		PathBuf::from (
			restore_matches.value_of_os (
				"source",
			).unwrap ());

	let target =
		PathBuf::from (
			restore_matches.value_of_os (
				"target",
			).unwrap ());

	Command::Restore (
		RestoreCommand {
			paths: paths,
			subvolume_id: subvolume_id,
			source: source,
			target: target,
		}
	)

}

fn scan_command (
	scan_matches: & clap::ArgMatches,
) -> Command {

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
			clap::SubCommand::with_name ("index")

			.arg (index_argument ())
			.arg (path_argument ())

			.about ("builds an index of btrfs nodes")

		)

		.subcommand (
			clap::SubCommand::with_name ("restore")

			.arg (subvolume_id_argument ())
			.arg (source_argument ())
			.arg (target_argument ())
			.arg (path_argument ())

			.about ("Restores files")

		)

		.subcommand (
			clap::SubCommand::with_name ("scan")

			.arg (path_argument ())

			.about ("Scans a filesystem")

		)

}

fn index_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("index")

		.long ("index")
		.value_name ("INDEX")
		.required (false)

		.help ("Index file")


}

fn subvolume_id_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("subvolume-id")

		.long ("subvolume-id")
		.value_name ("SUBVOLUME-ID")
		.required (true)

		.help ("Subvolume ID to restore")


}

fn path_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("path")

		.value_name ("PATH")
		.required (true)
		.multiple (true)

		.help ("Path to the BTRFS image(s) to recover")


}

fn source_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("source")

		.long ("source")
		.value_name ("SOURCE")
		.required (true)

		.help ("Source path to restore files from")

}

fn target_argument <'a, 'b> (
) -> clap::Arg <'a, 'b> {

	clap::Arg::with_name ("target")

		.long ("target")
		.value_name ("TARGET")
		.required (true)

		.help ("Target path to restore files to")

}

// ex: noet ts=4 filetype=rust
