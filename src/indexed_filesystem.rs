use std::cmp;
use std::collections::HashMap;
use std::hash;
use std::mem;
use std::path::Path;

use btrfs::diskformat::*;

use output::Output;

use super::index::*;

pub struct IndexedFilesystem <'a> {

	pub filesystem: & 'a BtrfsFilesystem <'a>,

	pub nodes: Vec <BtrfsNode <'a>>,
	pub items: Vec <BtrfsLeafItem <'a>>,

	pub internal_nodes: Vec <BtrfsInternalNode <'a>>,
	pub leaf_nodes: Vec <BtrfsLeafNode <'a>>,

	pub internal_items_by_tree:
		HashMap <BtrfsTreeId, Vec <& 'a BtrfsInternalItem>>,

	pub chunk_items: Vec <BtrfsChunkItem <'a>>,
	pub chunk_items_index: HashMap <u64, Vec <BtrfsChunkItem <'a>>>,

	pub dir_item_entries: Vec <BtrfsDirItemEntry <'a>>,
	pub dir_item_entries_index: HashMap <u64, Vec <BtrfsDirItemEntry <'a>>>,
	pub dir_item_entries_recent: HashMap <u64, BtrfsDirItemEntry <'a>>,
	pub dir_item_entries_by_parent: HashMap <u64, Vec <u64>>,

	pub extent_datas: Vec <BtrfsExtentData <'a>>,
	pub extent_datas_index: HashMap <u64, Vec <BtrfsExtentData <'a>>>,

	pub extent_items: Vec <BtrfsExtentItem <'a>>,
	pub extent_items_index: HashMap <u64, Vec <BtrfsExtentItem <'a>>>,

	pub inode_items: Vec <BtrfsInodeItem <'a>>,
	pub inode_items_index: HashMap <u64, Vec <BtrfsInodeItem <'a>>>,
	pub inode_items_recent: HashMap <u64, BtrfsInodeItem <'a>>,

	pub root_items: Vec <BtrfsRootItem <'a>>,
	pub root_items_index: HashMap <u64, Vec <BtrfsRootItem <'a>>>,

	pub root_tree_nodes: Vec <BtrfsNode <'a>>,

}

impl <'a> IndexedFilesystem <'a> {

	#[ inline ]
	pub fn open <
		IndexPath: AsRef <Path>,
	> (
		output: & Output,
		filesystem: & 'a BtrfsFilesystem <'a>,
		index_path: IndexPath,
	) -> Result <IndexedFilesystem <'a>, String> {

		Self::open_real (
			output,
			filesystem,
			index_path.as_ref (),
		)

	}

	pub fn open_real (
		output: & Output,
		filesystem: & 'a BtrfsFilesystem <'a>,
		index_path: & Path,
	) -> Result <IndexedFilesystem <'a>, String> {

		let mut indexed_filesystem =
			IndexedFilesystem {

			filesystem: filesystem,

			nodes: Vec::new (),
			items: Vec::new (),

			internal_nodes: Vec::new (),
			leaf_nodes: Vec::new (),

			internal_items_by_tree: HashMap::new (),

			chunk_items: Vec::new (),
			chunk_items_index: HashMap::new (),

			dir_item_entries: Vec::new (),
			dir_item_entries_index: HashMap::new (),
			dir_item_entries_recent: HashMap::new (),
			dir_item_entries_by_parent: HashMap::new (),

			extent_datas: Vec::new (),
			extent_datas_index: HashMap::new (),

			extent_items: Vec::new (),
			extent_items_index: HashMap::new (),

			inode_items: Vec::new (),
			inode_items_index: HashMap::new (),
			inode_items_recent: HashMap::new (),

			root_items: Vec::new (),
			root_items_index: HashMap::new (),

			root_tree_nodes: Vec::new (),

		};

		// create well-known trees

		for tree_id in vec! [
			BTRFS_ROOT_TREE_OBJECT_ID,
			BTRFS_EXTENT_TREE_OBJECT_ID,
			BTRFS_CHUNK_TREE_OBJECT_ID,
			BTRFS_DEV_TREE_OBJECT_ID,
			BTRFS_FS_TREE_OBJECT_ID,
		] {

			indexed_filesystem.internal_items_by_tree.insert (
				BtrfsTreeId::from (tree_id),
				Vec::new ());

		}

		// load index

		let output_job =
			output_job_start! (
				output,
				"Loading index from {}",
				index_path.to_string_lossy ());

		let node_positions =
			index_load (
				index_path,
			) ?;

		output_job.complete ();

/*
		let btrfs_device =
			filesystem.device (
				devices.superblock ().device_id (),
			).unwrap ();

		// create indexes

		let mut indexes =
			FilesystemIndexes::new (
				devices.superblock (),
			);

		indexes.add_nodes (
			output,
			& btrfs_device,
			& node_positions);

		indexes.build_inode_items_index (
			output,
			& btrfs_device);

		indexes.build_dir_items_index (
			output,
			& btrfs_device);

		// output statistics

		output_message! (
			output,
			"Nodes: {}",
			indexes.nodes.len ());

		output_message! (
			output,
			"  Internal: {}",
			indexes.internal_nodes.len ());

		output_message! (
			output,
			"  Leaf: {}",
			indexes.leaf_nodes.len ());

		output_message! (
			output,
			"Items: {}",
			indexes.nodes.len ());

		output_message! (
			output,
			"Root tree:");

		output_message! (
			output,
			"  Nodes: {}",
			indexes.root_tree_nodes.len ());

		output_message! (
			output,
			"  Items: {}",
			indexes.root_items.len ());

		// find root nodes

		let output_job =
			output_job_start! (
				output,
				"Looking for root trees");

		let mut root_node_addresses: HashSet <BtrfsPhysicalAddress> =
			indexes.root_tree_nodes.iter ().map (
				|node| node.physical_address ()
			).collect ();

		output_message! (
			output,
			"  All root nodes: {}",
			root_node_addresses.len ());;

		for root_internal_item
		in indexes.root_tree_internal_items () {

			if let Some (physical_address) =
				chunk_tree.logical_to_physical_address (
					root_internal_item.block_number ()) {

				root_node_addresses.remove (
					& physical_address);

			}

		}

		output_message! (
			output,
			"  Unreferenced root nodes: {}",
			root_node_addresses.len ());

		let root_nodes =
			root_node_addresses.iter ().map (
				|physical_address|

			devices.node_at_physical_address (
				* physical_address,
			).unwrap ()

		);

		let mut root_node_generations: HashSet <u64> =
			HashSet::new ();

		let mut duplicated_root_node_generations: HashSet <u64> =
			HashSet::new ();

		for root_node in root_nodes {

			if BtrfsTree::read_tree_physical_address (
				devices,
				& chunk_tree,
				root_node.physical_address (),
			).is_err () {
				continue;
			}

			if root_node_generations.contains (
				& root_node.generation ()) {

				duplicated_root_node_generations.insert (
					root_node.generation ());

			} else {

				root_node_generations.insert (
					root_node.generation ());

			}

		}

		output_message! (
			output,
			"  Duplicated root node generations: {}",
			duplicated_root_node_generations.len ());

		output_job.complete ();

		for root_item
		in indexes.root_items.iter ().filter (
			|item|

			item.object_id () == BTRFS_ROOT_TREE_OBJECT_ID

		) {

			println! (
				"Root item: {:?}",
				root_item);

		}

		// find root nodes

		for root_node
		in indexes.root_nodes.iter ().filter (
			|node| node.level () == sup {

			println! (
				"Root node: {:?}",
				root_node);

		}

		// find recent extent tree

		{

			let extent_tree_node =
				indexes.internal_items_by_tree.get (
					& 1,
				).unwrap ().iter ().filter (
					|item|

					item.key ().object_id () == 2
					&& item.key ().item_type () == 132

				).max_by_key (
					|item|

					item.generation ()

				).unwrap ();

			println! (
				"Extent tree node: {:?}",
				extent_tree_node);

		}

		// iterate root items

		for root_item in indexes.root_items.iter () {

			println! (
				"Root item: {:?}",
				root_item);

		}
*/

		// return

		Ok (indexed_filesystem)

	}

	pub fn add_nodes (
		& mut self,
		output: & Output,
		btrfs_device: & 'a BtrfsDevice <'a>,
		node_positions: & [usize],
	) {

		let output_job =
			output_job_start! (
				output,
				"Adding nodes from index");

		let node_position_total = node_positions.len () as u64;
		let mut node_position_count: u64 = 0;

		for node_position in node_positions.iter () {

			output_job.progress (
				node_position_count,
				node_position_total);

			let node_position = * node_position;

			let node_bytes =
				btrfs_device.slice_at (
					node_position as usize,
					self.filesystem.superblock ().node_size () as usize,
				).unwrap ();

			let node_result =
				BtrfsNode::from_bytes (
					BtrfsPhysicalAddress::new (
						btrfs_device.device_id (),
						node_position as u64),
					node_bytes);

			if node_result.is_err () {

				output_message! (
					output,
					"Error reading node at 0x{:x}: {}",
					node_position,
					node_result.err ().unwrap ());

				continue;

			}

			let node =
				node_result.unwrap ();

			self.nodes.push (
				node.clone ());

			output_debug! (
				output,
				"Node: {:?}",
				node);

			if node.tree_id ().is_root () {

				self.root_tree_nodes.push (
					node.clone ());

			}

			match node {

				BtrfsNode::Leaf (leaf_node) =>
					self.store_leaf_node (
						leaf_node),

				BtrfsNode::Internal (internal_node) =>
					self.store_internal_node (
						internal_node),

			};

			node_position_count += 1;

		}

		output_job.complete ();

		// sort leaves

		let output_job =
			output_job_start! (
				output,
				"Sorting nodes");

		Self::sort_leaves (
			& mut self.chunk_items_index);

		Self::sort_leaves (
			& mut self.dir_item_entries_index);

		Self::sort_leaves (
			& mut self.inode_items_index);

		Self::sort_leaves (
			& mut self.extent_datas_index);

		output_job.complete ();

		// try and construct chunk tree

		let mut num_unique: u64 = 0;
		let mut num_duplicated: u64 = 0;

		for (object_id, chunk_items) in & self.chunk_items_index {

			if chunk_items.len () == 1 {
				num_unique += 1;
			} else {
				num_duplicated += 1;
			}

		}

		output_message! (
			output,
			"CHUNK ITEMS: {} unique, {} duplicated",
			num_unique,
			num_duplicated);

		// output and return

		output_message! (
			output,
			"Found {} dir entries, {} inodes, {} extents",
			self.dir_item_entries.len (),
			self.inode_items.len (),
			self.extent_datas.len ());

	}

	fn store_leaf_node (
		& mut self,
		leaf_node: BtrfsLeafNode <'a>,
	) {

		self.leaf_nodes.push (
			leaf_node);

		for item in leaf_node.items () {

			match item {

				BtrfsLeafItem::ChunkItem (chunk_item) =>
					self.store_chunk_item (
						chunk_item),

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

				BtrfsLeafItem::RootItem (root_item) =>
					self.store_root_item (
						root_item),

				_ => (),

			}

		}

	}

	fn store_internal_node (
		& mut self,
		internal_node: BtrfsInternalNode <'a>,
	) {

		self.internal_nodes.push (
			internal_node);

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

	fn store_chunk_item (
		& mut self,
		chunk_item: BtrfsChunkItem <'a>,
	) {

		self.chunk_items.push (
			chunk_item);

		self.chunk_items_index.entry (
			chunk_item.key ().object_id (),
		).or_insert (
			Vec::new (),
		).push (
			chunk_item
		);

	}

	fn store_dir_item (
		& mut self,
		dir_item: BtrfsDirItem <'a>,
	) {

		for dir_item_entry in dir_item.entries () {

			self.store_dir_item_entry (
				dir_item_entry);

		}

	}

	fn store_dir_item_entry (
		& mut self,
		dir_item_entry: BtrfsDirItemEntry <'a>,
	) {

		self.dir_item_entries.push (
			dir_item_entry);

		self.dir_item_entries_index.entry (
			dir_item_entry.key ().object_id (),
		).or_insert (
			Vec::new (),
		).push (
			dir_item_entry
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

	fn store_root_item (
		& mut self,
		root_item: BtrfsRootItem <'a>,
	) {

		self.root_items.push (
			root_item);

		self.root_items_index.entry (
			root_item.object_id (),
		).or_insert (
			Vec::new (),
		).push (
			root_item
		)

	}

	pub fn build_inode_items_index (
		& mut self,
		output: & Output,
		btrfs_device: & 'a BtrfsDevice <'a>,
	) {

		let output_job =
			output_job_start! (
				output,
				"Selecting most recent inode items");

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

		output_job.complete ();

	}

	pub fn build_dir_items_index (
		& mut self,
		output: & Output,
		btrfs_device: & 'a BtrfsDevice <'a>,
	) {

		let output_job =
			output_job_start! (
				output,
				"Selecting most recent directory item entries");

		for dir_item_entry in self.dir_item_entries.iter () {

			let map_dir_item_entry =
				self.dir_item_entries_recent.entry (
					dir_item_entry.child_object_id (),
				).or_insert (
					* dir_item_entry
				);

			if map_dir_item_entry.transaction_id ()
				< dir_item_entry.transaction_id () {

				* map_dir_item_entry =
					* dir_item_entry;

			}

		}

		output_job.complete ();

		let output_job =
			output_job_start! (
				output,
				"Grouping directory item entries by parent");

		for dir_item_entry in self.dir_item_entries_recent.values () {

			self.dir_item_entries_by_parent.entry (
				dir_item_entry.object_id (),
			).or_insert (
				Vec::new (),
			).push (
				dir_item_entry.child_object_id (),
			);

		}

		output_job.complete ();

	}

	pub fn root_tree_internal_items (
		& 'a self,
	) -> & [& 'a BtrfsInternalItem] {

		self.internal_items_by_tree.get (
			& BTRFS_ROOT_TREE_ID,
		).unwrap ()

	}

	// property accessors

	pub fn dir_item_entries_recent (
		& 'a self,
	) -> & HashMap <u64, BtrfsDirItemEntry <'a>> {
		& self.dir_item_entries_recent
	}

	pub fn dir_item_entries_by_parent (
		& 'a self,
	) -> & HashMap <u64, Vec <u64>> {
		& self.dir_item_entries_by_parent
	}

	pub fn extent_datas_index (
		& 'a self,
	) -> & HashMap <u64, Vec <BtrfsExtentData>> {
		& self.extent_datas_index
	}

	pub fn extent_items_index (
		& 'a self,
	) -> & HashMap <u64, Vec <BtrfsExtentItem>> {
		& self.extent_items_index
	}

	pub fn inode_items_recent (
		& 'a self,
	) -> & HashMap <u64, BtrfsInodeItem <'a>> {
		& self.inode_items_recent
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
