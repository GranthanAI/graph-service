class GraphServiceException(Exception):
    """Base exception class for all Graph Service domain exceptions."""
    pass

class NodeNotFoundError(GraphServiceException):
    """Exception raised when a requested conversation node does not exist in the graph."""
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        super().__init__(f"Conversation node with ID '{node_id}' not found in the graph.")
