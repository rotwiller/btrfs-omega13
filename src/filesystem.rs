use std::cmp;
use std::collections::HashMap;
use std::hash;
use std::path::Path;

use btrfs::diskformat::*;

use output::Output;

use index::*;

pub struct Filesystem <'a> {
	devices: & 'a BtrfsDeviceMap,
	chunk_tree: BtrfsChunkTree <'a>,
	root_tree: BtrfsRootTree,
	indexes: FilesystemIndexes <'a>,
}

pub struct FilesystemIndexes <'a> {

	superblock: & 'a BtrfsSuperblock,

	nodes: Vec <BtrfsNode <'a>>,
	items: Vec <BtrfsLeafItem <'a>>,

	internal_items_by_tree: HashMap <u64, Vec <& 'a BtrfsInternalItem>>,

	extent_items: Vec <BtrfsExtentItem <'a>>,
	extent_items_index: HashMap <u64, Vec <BtrfsExtentItem <'a>>>,

	inode_items: Vec <BtrfsInodeItem <'a>>,
	inode_items_index: HashMap <u64, Vec <BtrfsInodeItem <'a>>>,
	inode_items_recent: HashMap <u64, BtrfsInodeItem <'a>>,

	dir_items: Vec <BtrfsDirItem <'a>>,
	dir_items_index: HashMap <u64, Vec <BtrfsDirItem <'a>>>,
	dir_items_recent: HashMap <u64, BtrfsDirItem <'a>>,
	dir_items_by_parent: HashMap <u64, Vec <u64>>,

	extent_datas: Vec <BtrfsExtentData <'a>>,
	extent_datas_index: HashMap <u64, Vec <BtrfsExtentData <'a>>>,

}

impl <'a> Filesystem <'a> {

	pub fn load_with_index (
		output: & mut Output,
		index_path: & Path,
		devices: & 'a BtrfsDeviceMap,
	) -> Result <Filesystem <'a>, String> {

		// load index

		output.status_format (
			format_args! (
				"Loading index from {} ...",
				index_path.to_string_lossy ()));

		let node_positions =
			index_load (
				index_path,
			) ?;

		output.status_done ();

		// read superblock

		let device =
			devices.get (
				& 1,
			).unwrap ();

		let superblock = unsafe {
			& * (
				device.pointer ().offset (
					0x1_0000,
				) as * const BtrfsSuperblock
			)
		};

		if superblock.magic () != BTRFS_MAGIC {

			return Err (
				"Superblock not found".to_owned ());

		}

		// read chunk tree

		output.message (
			"Attempting to read chunk tree");

		let chunk_tree =
			BtrfsChunkTree::new (
				& devices,
				& superblock,
			) ?;

		output.message_format (
			format_args! (
				"Chunk tree logical address: 0x{:x}",
				superblock.chunk_tree_logical_address ()));

		let (chunk_tree_device, chunk_tree_physical_address) =
			chunk_tree.logical_to_physical_address (
				superblock.chunk_tree_logical_address (),
			).unwrap ();

		output.message_format (
			format_args! (
				"Chunk tree physical device: {}",
				chunk_tree_device));

		output.message_format (
			format_args! (
				"Chunk tree physical address: 0x{:x}",
				chunk_tree_physical_address));

		// read root tree

		output.message (
			"Attempting to read root tree");

		let root_tree =
			BtrfsRootTree::new (
				& devices,
				& superblock,
				& chunk_tree,
			) ?;

		// create indexes

		let mut indexes =
			FilesystemIndexes {

			superblock: superblock,

			nodes: Vec::new (),
			items: Vec::new (),

			internal_items_by_tree: HashMap::new (),

			extent_datas: Vec::new (),
			extent_datas_index: HashMap::new (),

			extent_items: Vec::new (),
			extent_items_index: HashMap::new (),

			dir_items: Vec::new (),
			dir_items_index: HashMap::new (),
			dir_items_recent: HashMap::new (),
			dir_items_by_parent: HashMap::new (),

			inode_items: Vec::new (),
			inode_items_index: HashMap::new (),
			inode_items_recent: HashMap::new (),

		};

		// build indexes

		indexes.add_nodes (
			output,
			& devices,
			& node_positions);

		indexes.build_inode_items_index (
			output,
			& devices);

		indexes.build_dir_items_index (
			output,
			& devices);

		// return

		Ok (Filesystem {
			devices: devices,
			chunk_tree: chunk_tree,
			root_tree: root_tree,
			indexes: indexes,
		})

	}

	// property accessors

	pub fn dir_items_recent (
		& 'a self,
	) -> & HashMap <u64, BtrfsDirItem <'a>> {
		& self.indexes.dir_items_recent
	}

	pub fn dir_items_by_parent (
		& 'a self,
	) -> & HashMap <u64, Vec <u64>> {
		& self.indexes.dir_items_by_parent
	}

	pub fn extent_datas_index (
		& 'a self,
	) -> & HashMap <u64, Vec <BtrfsExtentData>> {
		& self.indexes.extent_datas_index
	}

	pub fn extent_items_index (
		& 'a self,
	) -> & HashMap <u64, Vec <BtrfsExtentItem>> {
		& self.indexes.extent_items_index
	}

	pub fn inode_items_recent (
		& 'a self,
	) -> & HashMap <u64, BtrfsInodeItem <'a>> {
		& self.indexes.inode_items_recent
	}

}

impl <'a> FilesystemIndexes <'a> {

	pub fn add_nodes (
		& mut self,
		output: & Output,
		devices: & 'a BtrfsDeviceMap,
		node_positions: & [usize],
	) {

		output.status (
			"Adding nodes from index ...");

		let device =
			devices.get (
				& 1,
			).unwrap ();

		let node_position_total = node_positions.len () as u64;
		let mut node_position_count: u64 = 0;

		for node_position in node_positions.iter () {

			output.status_progress (
				node_position_count,
				node_position_total);

			let node_position = * node_position;

			let node_bytes =
				device.slice_at (
					node_position as usize,
					self.superblock.node_size () as usize,
				);

			let node_result =
				BtrfsNode::from_bytes (
					/*node_position,*/
					node_bytes);

			if node_result.is_err () {

				output.message_format (
					format_args! (
						"Error reading node at 0x{:x}: {}",
						node_position,
						node_result.err ().unwrap ()));

				continue;

			}

			let node =
				node_result.unwrap ();

			self.nodes.push (
				node.clone ());

			match node {

				BtrfsNode::Leaf (leaf_node) =>
					self.store_leaf_node (
						leaf_node),

				BtrfsNode::Internal (internal_node) =>
					self.store_internal_node (
						internal_node),

			}

			node_position_count += 1;

		}

		output.status_done ();

		// sort leaves

		output.status (
			"Sorting nodes");

		Self::sort_leaves (
			& mut self.dir_items_index);

		Self::sort_leaves (
			& mut self.inode_items_index);

		Self::sort_leaves (
			& mut self.extent_datas_index);

		output.status_done ();

		// output and return

		output.message_format (
			format_args! (
				"Found {} dir entries, {} inodes, {} extents",
				self.dir_items.len (),
				self.inode_items.len (),
				self.extent_datas.len ()));

	}

	fn store_leaf_node (
		& mut self,
		leaf_node: BtrfsLeafNode <'a>,
	) {

		for item in leaf_node.items () {

			match item {

				BtrfsLeafItem::DirItem (dir_item) =>
					self.store_dir_item (
						dir_item),

				BtrfsLeafItem::ExtentData (extent_data) =>
					self.store_extent_data (
						extent_data),

				BtrfsLeafItem::ExtentItem (extent_item) =>
					self.store_extent_item (
						extent_item),

				BtrfsLeafItem::InodeItem (inode_item) =>
					self.store_inode_item (
						inode_item),

				_ => (),

			}

		}

	}

	fn store_internal_node (
		& mut self,
		internal_node: BtrfsInternalNode <'a>,
	) {

		let tree_items =
			self.internal_items_by_tree.entry (
				internal_node.header ().tree_id (),
			).or_insert (
				Vec::new (),
			);

		for item in internal_node.items () {

			tree_items.push (
				item);

		}

	}

	fn store_dir_item (
		& mut self,
		dir_item: BtrfsDirItem <'a>,
	) {

		self.dir_items.push (
			dir_item);

		self.dir_items_index.entry (
			dir_item.key ().object_id (),
		).or_insert (
			Vec::new (),
		).push (
			dir_item
		);

	}

	fn store_extent_data (
		& mut self,
		extent_data: BtrfsExtentData <'a>,
	) {

		self.extent_datas.push (
			extent_data);

		self.extent_datas_index.entry (
			extent_data.object_id (),
		).or_insert (
			Vec::new (),
		).push (
			extent_data
		);

	}

	fn store_extent_item (
		& mut self,
		extent_item: BtrfsExtentItem <'a>,
	) {

		self.extent_items.push (
			extent_item);

		self.extent_items_index.entry (
			extent_item.object_id (),
		).or_insert (
			Vec::new (),
		).push (
			extent_item
		);

	}

	fn store_inode_item (
		& mut self,
		inode_item: BtrfsInodeItem <'a>,
	) {

		self.inode_items.push (
			inode_item);

		self.inode_items_index.entry (
			inode_item.object_id (),
		).or_insert (
			Vec::new (),
		).push (
			inode_item
		)

	}

	pub fn build_inode_items_index (
		& mut self,
		output: & Output,
		devices: & BtrfsDeviceMap,
	) {

		output.status (
			"Selecting most recent inode items ...");

		for inode_item
		in self.inode_items.iter () {

			let map_inode_item =
				self.inode_items_recent.entry (
					inode_item.object_id (),
				).or_insert (
					* inode_item
				);

			if map_inode_item.transaction_id ()
				< inode_item.transaction_id () {

				* map_inode_item =
					* inode_item;

			}

		}

		output.status_done ();

	}

	pub fn build_dir_items_index (
		& mut self,
		output: & Output,
		devices: & BtrfsDeviceMap,
	) {

		output.status (
			"Selecting most recent directory items ...");

		for dir_item in self.dir_items.iter () {

			let map_dir_item =
				self.dir_items_recent.entry (
					dir_item.child_object_id (),
				).or_insert (
					* dir_item
				);

			if map_dir_item.transaction_id ()
				< dir_item.transaction_id () {

				* map_dir_item =
					* dir_item;

			}

		}

		output.status_done ();

		output.status (
			"Grouping directory items by parent ...");

		for dir_item in self.dir_items_recent.values () {

			self.dir_items_by_parent.entry (
				dir_item.object_id (),
			).or_insert (
				Vec::new (),
			).push (
				dir_item.child_object_id ());

		}

		output.status_done ();

	}

	// utility functions

	fn sort_leaves <
		Key: hash::Hash + cmp::Eq,
		Value: cmp::Ord,
	> (
		map: & mut HashMap <Key, Vec <Value>>,
	) {

		for (ref key, ref mut values)
		in map.iter_mut () {

			values.sort ();

		}

	}

}

// ex: noet ts=4 filetype=rust
