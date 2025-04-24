class RoundRobinBalancer:
    """Simple round-robin load balancer for distributing messages across multiple queues"""
    
    def __init__(self, target_queues):
        """Initialize with a list of target queue names"""
        self.target_queues = target_queues
        self.current_index = 0
        
    def next_queue(self):
        """Return the next queue in round-robin fashion"""
        if not self.target_queues:
            return None
            
        selected_queue = self.target_queues[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.target_queues)
        return selected_queue
