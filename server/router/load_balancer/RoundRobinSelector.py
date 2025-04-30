from load_balancer.BaseQueueSelector import BaseQueueSelector

class RoundRobinSelector(BaseQueueSelector):
    """Distributes entire data batches to queues in a round-robin fashion"""
    
    def initialize(self):
        """Initialize the round-robin counter"""
        self.current_index = 0
        
        # Flatten the queue structure if it's nested
        if self.target_queues and isinstance(self.target_queues[0], list):
            # If we have nested queues but only one shard, flatten it
            if len(self.target_queues) == 1:
                self.target_queues = self.target_queues[0]
            else:
                raise ValueError("RoundRobinSelector expects a flat list of queues or a single shard")
    
    def select_target_queues(self, data_batch):
        """
        Select a single target queue in round-robin fashion
        
        Returns:
            dict: A mapping with a single queue name to the entire data batch
        """
        if not self.target_queues:
            return {}
            
        selected_queue = self.target_queues[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.target_queues)
        
        return {selected_queue: data_batch}