from load_balancer.BaseQueueSelector import BaseQueueSelector

class ShardingSelector(BaseQueueSelector):
    """
    Distributes data across multiple shards based on content,
    and then distributes within each shard using round-robin
    """
    
    def initialize(self):
        """Initialize the shard mapping and round-robin counters"""
        # Validate that we have a nested structure
        if not self.target_queues or not isinstance(self.target_queues[0], list):
            # Convert flat list to a single shard
            self.target_queues = [self.target_queues] if self.target_queues else [[]]
            
        # Create a counter for each shard for round-robin distribution
        self.shard_counters = [0 for _ in range(len(self.target_queues))]
        
        # Setup character to shard mapping
        self._build_character_map()
    
    def _build_character_map(self):
        """Build a mapping from characters to shard indices"""
        self.char_map = {}
        
        # Define the alphabet plus numbers for hashing
        characters = "abcdefghijklmnopqrstuvwxyz0123456789"
        
        # Distribute characters evenly among shards
        shard_count = len(self.target_queues)
        for i, char in enumerate(characters):
            shard_index = i % shard_count
            self.char_map[char] = shard_index
        
        # Default case for any other characters
        self.default_shard = 0
    
    def get_shard_index(self, item):
        """
        Determine which shard an item belongs to based on its name
        
        Returns:
            int: Index of the target shard
        """
        if not item or not isinstance(item, dict) or 'name' not in item:
            return self.default_shard
            
        name = item.get('name', '')
        if not name or not isinstance(name, str):
            return self.default_shard
            
        first_char = name[0].lower()
        return self.char_map.get(first_char, self.default_shard)
    
    def select_queue_from_shard(self, shard_index):
        """Select a queue from the given shard using round-robin"""
        shard = self.target_queues[shard_index]
        if not shard:
            return None
            
        selected_queue = shard[self.shard_counters[shard_index]]
        self.shard_counters[shard_index] = (self.shard_counters[shard_index] + 1) % len(shard)
        
        return selected_queue
    
    def select_target_queues(self, data_batch):
        """
        Distribute data items across shards based on content,
        then round-robin within each shard
        
        Returns:
            dict: A mapping from queue names to lists of data items
        """
        
        distribution = {}
        
        for item in data_batch:
            shard_index = self.get_shard_index(item)
            queue = self.select_queue_from_shard(shard_index)
            
            if queue not in distribution:
                distribution[queue] = []
            
            distribution[queue].append(item)
            
        return distribution
