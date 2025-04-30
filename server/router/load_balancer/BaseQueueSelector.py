class BaseQueueSelector:
    """Base class for queue selection strategies"""
    
    def __init__(self, target_queues):
        """
        Initialize with target queues
        
        Args:
            target_queues: Can be a flat list of queue names or a nested list where each inner
                          list represents a shard containing multiple worker queues
        """
        self.target_queues = target_queues
        self.initialize()
        
    def initialize(self):
        """Initialize any strategy-specific data structures"""
        pass
        
    def select_target_queues(self, data_batch):
        """
        Select target queues for the given data batch
        
        Args:
            data_batch: A batch of data items
            
        Returns:
            dict: A mapping from queue names to lists of data items
        """
        raise NotImplementedError("Subclasses must implement select_target_queues")
