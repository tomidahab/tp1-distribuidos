import logging
from load_balancer.RoundRobinBalancer import RoundRobinBalancer

def create_balancer(balancer_type, output_queues):
    """Create a balancer based on the specified type
    
    Args:
        balancer_type (str): Type of balancer to create (e.g., "round_robin", "hash_function")
        output_queues (list): List of output queue names
        
    Returns:
        A balancer instance that implements a next_queue() method
    """
    balancer_type = balancer_type.lower()
    
    if balancer_type == "round_robin":
        return RoundRobinBalancer(output_queues)
    elif balancer_type == "hash_function":
        # Future implementation
        # return HashFunctionBalancer(output_queues)
        logging.warning("HashFunctionBalancer balancer not yet implemented, falling back to RoundRobinBalancer")
        return RoundRobinBalancer(output_queues)
    else:
        logging.warning(f"Unknown balancer type '{balancer_type}', falling back to RoundRobinBalancer")
        return RoundRobinBalancer(output_queues)
