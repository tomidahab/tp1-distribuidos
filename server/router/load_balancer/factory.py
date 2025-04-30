import logging
from load_balancer.RoundRobinSelector import RoundRobinSelector
from load_balancer.ShardingSelector import ShardingSelector

def create_balancer(balancer_type, output_queues):
    """Create a balancer based on the specified type
    
    Args:
        balancer_type (str): Type of balancer to create (e.g., "shard_by_ascii", "round_robin")
        output_queues (list): List of output queue names or nested list for sharded queues
        
    Returns:
        A balancer instance that implements select_target_queues() method
    """
    balancer_type = balancer_type.lower() if balancer_type else ""
    
    if balancer_type == "shard_by_ascii":
        logging.info(f"Creating ShardingSelector with {len(output_queues)} shards")
        return ShardingSelector(output_queues)
    elif balancer_type == "round_robin":
        logging.info(f"Creating RoundRobinSelector")
        return RoundRobinSelector(output_queues)
    else:
        # Default behavior depends on the structure of output_queues
        if output_queues and isinstance(output_queues, list) and isinstance(output_queues[0], list) and len(output_queues) > 1:
            # If we have a nested structure with multiple shards, default to sharding
            logging.info(f"Using default ShardingSelector for nested queue structure")
            return ShardingSelector(output_queues)
        else:
            # Otherwise use round-robin
            logging.info(f"Using default RoundRobinSelector")
            return RoundRobinSelector(output_queues)
