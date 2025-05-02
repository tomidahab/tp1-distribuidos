#!/usr/bin/env python3

import yaml
import argparse
from copy import deepcopy

def generate_docker_compose(config):
    """Generate a docker-compose file based on the provided configuration."""
    
    output = {"services": {}}
    services = output["services"]
    
    # Add static services
    add_static_services(services, config)
    
    # Add client replicas
    add_client_replicas(services, config["client_replicas"])
    
    # Add worker replicas for each worker type
    for worker_type, replicas in config["worker_replicas"].items():
        add_worker_replicas(services, worker_type, replicas, config)
    
    # Add routers with proper configuration based on worker counts
    configure_routers(services, config)
    
    return output

def add_static_services(services, config):
    """Add static services like rabbitmq and boundary."""
    # RabbitMQ service
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "ports": ["5672:5672", "15672:15672"]
    }
    
    # Boundary service
    services["boundary"] = {
        "build": {
            "context": "./server",
            "dockerfile": "boundary/Dockerfile"
        },
        "env_file": ["./server/boundary/.env"],
        "environment": [
            "MOVIES_ROUTER_QUEUE=boundary_movies_router",
            "MOVIES_ROUTER_Q5_QUEUE=boundary_movies_Q5_router",
            "CREDITS_ROUTER_QUEUE=boundary_credits_router",
            "RATINGS_ROUTER_QUEUE=boundary_ratings_router"
        ],
        "depends_on": ["rabbitmq"],
        "ports": ["5000:5000"],
        "volumes": [
            "./server/boundary:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

def add_client_replicas(services, replicas):
    """Add client services based on the specified replica count."""
    for i in range(1, replicas + 1):
        client_name = f"client{i}"
        services[client_name] = {
            "env_file": ["./client/.env"],
            "build": "./client",
            "environment": [f"CLIENT_ID={i}"],
            "depends_on": {
                "boundary": {
                    "condition": "service_started"
                }
            },
            "volumes": ["./client:/app"],
            "restart": "on-failure:3"  # Add restart policy
        }

def add_worker_replicas(services, worker_type, replicas, config):
    """Add worker services of a specific type based on the replica count."""
    
    # Define worker templates for different worker types
    templates = {
        "filter_by_year": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_year/Dockerfile"
            },
            "env_file": ["./server/worker/filter_by_year/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=filter_by_year_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=country_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/filter_by_year:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "restart": "on-failure:3"
        },
        # TODO: Add the the rabbitmq healthcheck dependency to the rest of the workers
        "filter_by_country": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_country/Dockerfile"
            },
            "env_file": ["./server/worker/filter_by_country/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=filter_by_country_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=join_movies_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/filter_by_country:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "join_credits": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_credits/Dockerfile"
            },
            "env_file": ["./server/worker/join_credits/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE_MOVIES=join_credits_worker_{i}_movies",
                "ROUTER_CONSUME_QUEUE_CREDITS=join_credits_worker_{i}_credits",
                "ROUTER_PRODUCER_QUEUE=count_router",
                f"NUMBER_OF_CLIENTS={config['client_replicas']}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_credits:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "join_ratings": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_ratings/Dockerfile"
            },
            "env_file": ["./server/worker/join_ratings/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE_MOVIES=join_ratings_worker_{i}_movies",
                "ROUTER_CONSUME_QUEUE_RATINGS=join_ratings_worker_{i}_ratings",
                "ROUTER_PRODUCER_QUEUE=average_movies_by_rating_router",
                f"NUMBER_OF_CLIENTS={config['client_replicas']}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_ratings:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "count": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/count/Dockerfile"
            },
            "env_file": ["./server/worker/count/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=count_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/count:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "sentiment_analysis": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/sentiment_analysis/Dockerfile"
            },
            "env_file": ["./server/worker/sentiment_analysis/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=sentiment_analysis_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/sentiment_analysis:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "deploy": {
                "resources": {
                    "limits": {
                        "memory": "2G"
                    }
                }
            }
        },
        # Adding missing worker types
        "average_movies_by_rating": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_movies_by_rating/Dockerfile"
            },
            "env_file": ["./server/worker/average_movies_by_rating/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=average_movies_by_rating_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=max_min_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/average_movies_by_rating:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "max_min": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/max_min/Dockerfile"
            },
            "env_file": ["./server/worker/max_min/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=max_min_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=max_min_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "collector_max_min": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_max_min/Dockerfile"
            },
            "env_file": ["./server/worker/collector_max_min/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_max_min_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "top": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/top/Dockerfile"
            },
            "env_file": ["./server/worker/top/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=top_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_10_actors_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/top:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "collector_top_10_actors": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_top_10_actors/Dockerfile"
            },
            "env_file": ["./server/worker/collector_top_10_actors/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_top_10_actors_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_top_10_actors:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "average_sentiment": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_sentiment/Dockerfile"
            },
            "env_file": ["./server/worker/average_sentiment/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=average_sentiment_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/average_sentiment:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        },
        "collector_average_sentiment": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_average_sentiment_worker/Dockerfile"
            },
            "env_file": ["./server/worker/collector_average_sentiment_worker/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_average_sentiment_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_average_sentiment_worker:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
    
    # Special case for collector workers that don't have numbered replicas
    collector_worker_types = ["collector_max_min", "collector_top_10_actors", "collector_average_sentiment"]
    if worker_type in collector_worker_types:
        # Only add if replicas > 0, and don't add a numbered suffix
        if replicas > 0:
            template = templates[worker_type]
            worker_name = worker_type + "_worker"
            services[worker_name] = deepcopy(template)
        return

    # Skip sentiment_analysis workers if they should be commented out
    if worker_type == "sentiment_analysis" and config.get("comment_sentiment_analysis", True):
        return

    # For all other worker types with replicas > 0, add numbered replicas
    if replicas > 0 and worker_type in templates:
        template = templates[worker_type]
        for i in range(1, replicas + 1):
            worker_name = f"{worker_type}_worker_{i}"
            worker_config = deepcopy(template)
            
            # Replace {i} placeholders in environment variables
            for j, env_var in enumerate(worker_config.get("environment", [])):
                worker_config["environment"][j] = env_var.replace("{i}", str(i))
            
            services[worker_name] = worker_config

def configure_routers(services, config):
    """Configure router services based on worker counts."""
    
    # First check if sentiment_analysis should be included
    include_sentiment_analysis = config["worker_replicas"]["sentiment_analysis"] > 0 and not config.get("comment_sentiment_analysis", True)
    
    # Configure year_movies_router
    year_filter_workers = config["worker_replicas"]["filter_by_year"]
    if year_filter_workers > 0:
        services["year_movies_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_movies_router",
                f"OUTPUT_QUEUES={','.join([f'filter_by_year_worker_{i}' for i in range(1, year_filter_workers + 1)])}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "restart": "on-failure:3"
        }
    # TODO: Add the rabbitmq healthcheck dependency to the rest of the routers
    # Only add movies_q5_router if sentiment_analysis is enabled
    if include_sentiment_analysis:
        sentiment_analysis_workers = config["worker_replicas"]["sentiment_analysis"]
        services["movies_q5_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_movies_Q5_router",
                f"OUTPUT_QUEUES={','.join([f'sentiment_analysis_worker_{i}' for i in range(1, sentiment_analysis_workers + 1)])}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Configure join_credits_router
    join_credits_workers = config["worker_replicas"]["join_credits"]
    if join_credits_workers > 0:
        services["join_credits_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_credits_router",
                f"OUTPUT_QUEUES={','.join([f'join_credits_worker_{i}_credits' for i in range(1, join_credits_workers + 1)])}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # Configure join_ratings_router
    join_ratings_workers = config["worker_replicas"]["join_ratings"]
    if join_ratings_workers > 0:
        services["join_ratings_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_ratings_router",
                f"OUTPUT_QUEUES={','.join([f'join_ratings_worker_{i}_ratings' for i in range(1, join_ratings_workers + 1)])}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Configure country_router
    filter_by_country_workers = config["worker_replicas"]["filter_by_country"]
    if filter_by_country_workers > 0:
        services["country_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=country_router",
                f"OUTPUT_QUEUES={','.join([f'filter_by_country_worker_{i}' for i in range(1, filter_by_country_workers + 1)])}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Configure join_movies_router
    join_credits_workers = config["worker_replicas"]["join_credits"]
    join_ratings_workers = config["worker_replicas"]["join_ratings"]
    
    if join_credits_workers > 0 or join_ratings_workers > 0:
        output_queues = []
        for i in range(1, join_ratings_workers + 1):
            output_queues.append(f"join_ratings_worker_{i}_movies")
        for i in range(1, join_credits_workers + 1):
            output_queues.append(f"join_credits_worker_{i}_movies")
            
        services["join_movies_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=join_movies_router",
                f"OUTPUT_QUEUES={','.join(output_queues)}",
                "EXCHANGE_TYPE=fanout",
                "EXCHANGE_NAME=join_router_exchange"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Configure count_router with shard distribution
    count_workers = config["worker_replicas"]["count"]
    if count_workers > 0:
        # Create count workers distribution for shard_by_ascii
        middle = count_workers // 2
        first_half = [f"count_worker_{i}" for i in range(1, middle + 1)]
        second_half = [f"count_worker_{i}" for i in range(middle + 1, count_workers + 1)]
        
        # Format using JSON format with double quotes
        if len(first_half) > 0 and len(second_half) > 0:
            output_queues = '[["{0}"],["{1}"]]'.format('","'.join(first_half), '","'.join(second_half))
        else:
            output_queues = '[["{0}"]]'.format('","'.join(first_half + second_half))
            
        services["count_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=count_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Configure routers for average_movies_by_rating based on the number of workers
    average_movies_by_rating_workers = config["worker_replicas"]["average_movies_by_rating"]
    if average_movies_by_rating_workers > 0:
        worker_list = [f"average_movies_by_rating_worker_{i}" for i in range(1, average_movies_by_rating_workers + 1)]
        output_queues = '[["{0}"]]'.format('","'.join(worker_list))
        
        services["average_movies_by_rating_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=average_movies_by_rating_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # Configure routers for max_min based on the number of workers
    max_min_workers = config["worker_replicas"]["max_min"]
    if max_min_workers > 0:
        worker_list = [f"max_min_worker_{i}" for i in range(1, max_min_workers + 1)]
        output_queues = '[["{0}"]]'.format('","'.join(worker_list))
        
        services["max_min_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=max_min_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # Configure routers for top based on the number of workers
    top_workers = config["worker_replicas"]["top"]
    if top_workers > 0:
        worker_list = [f"top_worker_{i}" for i in range(1, top_workers + 1)]
        output_queues = '[["{0}"]]'.format('","'.join(worker_list))
        
        services["top_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=4",
                "INPUT_QUEUE=top_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # Configure average_sentiment_router based on the number of workers
    average_sentiment_workers = config["worker_replicas"]["average_sentiment"]
    if average_sentiment_workers > 0 and not config.get("comment_sentiment_analysis", True):
        output_lists = []
        for i in range(1, average_sentiment_workers + 1):
            output_lists.append(f'["average_sentiment_worker_{i}"]')
        output_queues = f"[{','.join(output_lists)}]"
        
        services["average_sentiment_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=average_sentiment_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    # Add collector routers if their workers are enabled
    if config["worker_replicas"]["collector_max_min"] > 0:
        services["max_min_collector_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=max_min_collector_router",
                "OUTPUT_QUEUES=collector_max_min_worker",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    if config["worker_replicas"]["collector_top_10_actors"] > 0:
        services["top_10_actors_collector_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=top_10_actors_collector_router",
                "OUTPUT_QUEUES=collector_top_10_actors_worker",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    
    if config["worker_replicas"]["collector_average_sentiment"] > 0 and not config.get("comment_sentiment_analysis", True):
        services["average_sentiment_collector_router"] = {
            "build": {"context": "./server", "dockerfile": "router/Dockerfile"},
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=2",
                "INPUT_QUEUE=average_sentiment_collector_router",
                "OUTPUT_QUEUES=collector_average_sentiment_worker",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

def main():
    parser = argparse.ArgumentParser(description='Generate docker-compose configuration')
    parser.add_argument('--clients', type=int, default=3, help='Number of client replicas')
    parser.add_argument('--output', type=str, default='docker-compose_test.yaml', help='Output file')
    
    # Worker replica arguments
    parser.add_argument('--filter-by-year', type=int, default=2, help='Number of filter_by_year workers')
    parser.add_argument('--filter-by-country', type=int, default=2, help='Number of filter_by_country workers')
    parser.add_argument('--join-credits', type=int, default=2, help='Number of join_credits workers')
    parser.add_argument('--join-ratings', type=int, default=2, help='Number of join_ratings workers')
    parser.add_argument('--count', type=int, default=4, help='Number of count workers')
    parser.add_argument('--sentiment-analysis', type=int, default=2, help='Number of sentiment_analysis workers')
    parser.add_argument('--average-movies-by-rating', type=int, default=1, help='Number of average_movies_by_rating workers')
    parser.add_argument('--max-min', type=int, default=1, help='Number of max_min workers')
    parser.add_argument('--top', type=int, default=1, help='Number of top workers')
    parser.add_argument('--average-sentiment', type=int, default=2, help='Number of average_sentiment workers')
    parser.add_argument('--include-sentiment-analysis', action='store_true', help='Include sentiment_analysis workers (uncommented)')
    
    args = parser.parse_args()
    
    # Collectors always have exactly one replica, cannot be 0
    collector_max_min = 1
    collector_top_10_actors = 1
    collector_average_sentiment = 1
    
    config = {
        "client_replicas": args.clients,
        "worker_replicas": {
            "filter_by_year": args.filter_by_year,
            "filter_by_country": args.filter_by_country,
            "join_credits": args.join_credits,
            "join_ratings": args.join_ratings,
            "count": args.count,
            "sentiment_analysis": args.sentiment_analysis,
            "average_movies_by_rating": args.average_movies_by_rating,
            "max_min": args.max_min,
            "top": args.top,
            "average_sentiment": args.average_sentiment,
            "collector_max_min": collector_max_min,
            "collector_top_10_actors": collector_top_10_actors,
            "collector_average_sentiment": collector_average_sentiment
        },
        "comment_sentiment_analysis": not args.include_sentiment_analysis
    }
    
    docker_compose = generate_docker_compose(config)
    
    with open(args.output, 'w') as f:
        yaml.dump(docker_compose, f, default_flow_style=False, sort_keys=False)
    
    print(f"Docker Compose configuration generated in {args.output}")

if __name__ == "__main__":
    main()
