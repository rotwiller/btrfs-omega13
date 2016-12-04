use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

use btrfs::diskformat::*;

use memmap::Mmap;

use output::OutputBox;

type InodeItemAndKey <'a> = (
	& 'a BtrfsLeafNodeHeader,
	& 'a BtrfsInodeItem,
);

type DirItemAndKey <'a> = (
	& 'a BtrfsLeafNodeHeader,
	& 'a BtrfsDirItem,
);

pub struct Filesystem <'a> {

	positions: & 'a [usize],
	mmaps: & 'a [Mmap],

	item_index: HashMap <Rc <BtrfsKey>, Vec <usize>>,

	inode_items: Vec <InodeItemAndKey <'a>>,
	inode_items_index: HashMap <i64, Vec <InodeItemAndKey <'a>>>,
	inode_items_recent: HashMap <i64, InodeItemAndKey <'a>>,

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

			inode_items: Vec::new (),
			inode_items_index: HashMap::new (),
			inode_items_recent: HashMap::new (),

			dir_items: Vec::new (),
			dir_items_index: HashMap::new (),
			dir_items_recent: HashMap::new (),
			dir_items_by_parent: HashMap::new (),

		}

	}

	pub fn build_main_index (
		& mut self,
		output: & mut OutputBox,
	) {

		output.status (
			"Building main index ...");

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

					BTRFS_INODE_ITEM_TYPE => {

						let inode_item: & BtrfsInodeItem = unsafe {
							& * (
								mmap.ptr ().offset (
									item_data_position as isize,
								) as * const BtrfsInodeItem
							)
						};

						let inode_item_and_header = (
							leaf_node_header,
							inode_item,
						);

						self.inode_items.push (
							inode_item_and_header);

						self.inode_items_index.entry (
							item_key.object_id,
						).or_insert (
							Vec::new (),
						).push (
							inode_item_and_header);

					},

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

		output.status_done ();

	}

	pub fn build_inode_items_index (
		& mut self,
		output: & mut OutputBox,
	) {

		output.status (
			"Selecting most recent inode items ...");

		for & (leaf_node_header, inode_item)
		in self.inode_items.iter () {

			let map_inode_item_and_key =
				self.inode_items_recent.entry (
					leaf_node_header.key.object_id,
				).or_insert (
					(leaf_node_header, inode_item)
				);

			if map_inode_item_and_key.1.transid < inode_item.transid {

				* map_inode_item_and_key = (
					leaf_node_header,
					inode_item,
				);

			}

		}

		output.status_done ();

	}

	pub fn build_dir_items_index (
		& mut self,
		output: & mut OutputBox,
	) {

		output.status (
			"Selecting most recent directory items ...");

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

		output.status_done ();

		output.status (
			"Grouping directory items by parent ...");

		for & (leaf_node_header, dir_item)
		in self.dir_items_recent.values () {

			self.dir_items_by_parent.entry (
				leaf_node_header.key.object_id,
			).or_insert (
				Vec::new (),
			).push (
				dir_item.child_key.object_id);

		}

		output.status_done ();

	}

	pub fn inode_items_recent (
		& 'a self,
	) -> & HashMap <i64, InodeItemAndKey <'a>> {
		& self.inode_items_recent
	}

	pub fn dir_items_recent (
		& 'a self,
	) -> & HashMap <i64, DirItemAndKey <'a>> {
		& self.dir_items_recent
	}

	pub fn dir_items_by_parent (
		& 'a self,
	) -> & HashMap <i64, Vec <i64>> {
		& self.dir_items_by_parent
	}

}

// ex: noet ts=4 filetype=rust
