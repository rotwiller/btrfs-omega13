use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

use btrfs::diskformat::*;

use memmap::Mmap;
use memmap::Protection;

pub struct DeviceMaps {
	mmaps: Vec <Mmap>,
}

impl DeviceMaps {

	pub fn open (
		device_paths: & Vec <PathBuf>,
	) -> Result <DeviceMaps, String> {

		// open devices

		let mut mmaps: Vec <Mmap> =
			Vec::new ();

		for device_path in device_paths.iter () {

			let file = try! (
				File::open (
					device_path,
				).map_err (
					|error|

					format! (
						"Error opening {}: {}",
						device_path.to_string_lossy (),
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
						device_path.to_string_lossy (),
						error.description ())

				)
			);

			mmaps.push (
				mmap);

		}

		// return

		Ok (
			DeviceMaps {
				mmaps: mmaps,
			}
		)

	}

	pub fn get_data (
		& self,
	) -> Vec <BtrfsDevice> {

		self.mmaps.iter ().map (
			|mmap|

			BtrfsDevice::new (
				mmap.ptr (),
				mmap.len (),
			)

		).collect ()

	}

}

// ex: noet ts=4 filetype=rust
