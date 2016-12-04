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
use index::*;

type DirItemAndKey <'a> = (
	& 'a BtrfsLeafNodeHeader,
	& 'a BtrfsDirItem,
);

pub struct Filesystem <'a> {

	positions: & 'a [usize],
	mmaps: & 'a [Mmap],

	item_index: HashMap <Rc <BtrfsKey>, Vec <usize>>,

	dir_items: Vec <DirItemAndKey <'a>>,
	dir_items_index: HashMap <i64, Vec <DirItemAndKey <'a>>>,
	dir_items_recent: HashMap <i64, DirItemAndKey <'a>>,
	dir_items_by_parent: HashMap <i64, Vec <i64>>,

}

impl <'a> Filesystem <'a> {

	pub fn new (
		positions: & 'a [usize],
		mmaps: & 'a [Mmap],
	) -> Filesystem <'a> {

		Filesystem {

			positions: positions,
			mmaps: mmaps,

			item_index: HashMap::new (),

			dir_items: Vec::new (),
			dir_items_index: HashMap::new (),
			dir_items_recent: HashMap::new (),
			dir_items_by_parent: HashMap::new (),

		}

	}

	pub fn index (
		& mut self,
		output: & mut OutputBox,
	) {

		self.build_main_index (
			output);

		self.build_dir_items_recent (
			output);

		self.build_dir_items_by_parent (
			output);


	}

	fn build_main_index (
		& mut self,
		output: & mut OutputBox,
	) {

		let mmap =
			& self.mmaps [0];

		for position in self.positions {

			let position = * position;

			let node_header: & BtrfsNodeHeader = unsafe {
				& * (
					mmap.ptr ().offset (position as isize)
					as * const BtrfsNodeHeader
				)
			};

			if node_header.level != 0 {
				continue;
			}

			/*
			output.message (
				& format! (
					"Position 0x{:x}",
					position));
			*/

			for item_index in 0 .. node_header.num_items {

				let item_header_position =
					position
					+ mem::size_of::<BtrfsNodeHeader> ()
					+ mem::size_of::<BtrfsLeafNodeHeader> ()
						* item_index as usize;

				let leaf_node_header: & BtrfsLeafNodeHeader = unsafe {
					& * (
						mmap.ptr ().offset (
							item_header_position as isize,
						) as * const BtrfsLeafNodeHeader
					)
				};

				let item_key =
					Rc::new (
						leaf_node_header.key.clone ());

				let item_data_position =
					position
					+ mem::size_of::<BtrfsNodeHeader> ()
					+ leaf_node_header.data_offset as usize;

				self.item_index.entry (
					item_key.clone (),
				).or_insert (
					Vec::new (),
				).push (
					item_data_position,
				);

				/*
				output.message (
					& format! (
						"Item {:?} 0x{:x} 0x{:x}",
						item_key,
						leaf_node_header.data_offset,
						leaf_node_header.data_size));
				*/

				match leaf_node_header.key.item_type {

					BTRFS_DIR_INDEX_TYPE => {

						let dir_item: & BtrfsDirItem = unsafe {
							& * (
								mmap.ptr ().offset (
									item_data_position as isize,
								) as * const BtrfsDirItem
							)
						};

						let dir_item_and_header = (
							leaf_node_header,
							dir_item,
						);

						self.dir_items.push (
							dir_item_and_header);

						self.dir_items_index.entry (
							item_key.object_id,
						).or_insert (
							Vec::new (),
						).push (
							dir_item_and_header);

					},

					_ => (),

				}

			}

		}

	}

	fn build_dir_items_recent (
		& mut self,
		output: & mut OutputBox,
	) {

		for & (leaf_node_header, dir_item)
		in self.dir_items.iter () {

			let map_dir_item_and_key =
				self.dir_items_recent.entry (
					dir_item.child_key.object_id,
				).or_insert (
					(leaf_node_header, dir_item)
				);

			if map_dir_item_and_key.1.transid < dir_item.transid {

				* map_dir_item_and_key = (
					leaf_node_header,
					dir_item,
				);

			}

		}

	}

	fn build_dir_items_by_parent (
		& mut self,
		output: & mut OutputBox,
	) {

		for & (leaf_node_header, dir_item)
		in self.dir_items_recent.values () {

			self.dir_items_by_parent.entry (
				leaf_node_header.key.object_id,
			).or_insert (
				Vec::new (),
			).push (
				dir_item.child_key.object_id);

		}

	}

	pub fn print_roots (
		& mut self,
		output: & mut OutputBox,
	) {

		// find parent dir entries

		let root_object_ids: HashSet <i64> =
			self.dir_items_recent.values ().filter (
				|&& (leaf_node_header, dir_item)|

				! self.dir_items_recent.contains_key (
					& leaf_node_header.key.object_id)

			).map (
				|& (leaf_node_header, dir_item)|

				leaf_node_header.key.object_id

			).collect ();

		// print information about roots

		for root_object_id in root_object_ids {

			output.message (
				& format! (
					"ROOT: {}",
					root_object_id));

			self.print_tree (
				output,
				"  ",
				root_object_id);

		}

	}

	fn print_tree (
		& self,
		output: & mut OutputBox,
		indent: & str,
		object_id: i64,
	) {

		if let Some (child_object_ids) =
			self.dir_items_by_parent.get (
				& object_id) {

			let next_indent =
				format! (
					"{}  ",
					indent);

			for child_object_id
				in self.dir_items_by_parent.get (
					& object_id,
				).unwrap () {

				let & (child_leaf_node_header, child_dir_item) =
					self.dir_items_recent.get (
						child_object_id,
					).unwrap ();

				output.message (
					& format! (
						"{}{}",
						indent,
						String::from_utf8_lossy (
							child_dir_item.name ())));

				self.print_tree (
					output,
					& next_indent,
					* child_object_id);

			}

		}

	}

}

// ex: noet ts=4 filetype=rust
