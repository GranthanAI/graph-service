# Graph Service API Testing & Swagger Schema Guide

This document lists the REST endpoints, query parameters, request validations, response schemas, and sample requests to test Graph Service on the interactive Swagger UI interface.

---

## Interactive Swagger UI Access
When the local dev server is running (via `make run`), you can access the interactive Swagger UI documentation at:
- **URL**: `http://127.0.0.1:8000/docs`
- **Alternative Redoc**: `http://127.0.0.1:8000/redoc`

---

## 1. Health Check Endpoint

Verifies that the service is active and the Neo4j database is reachable.

- **Path**: `/graph/health`
- **Method**: `GET`
- **Auth**: None
- **Query Parameters**: None
- **Response Schema (`200 OK`)**:
  ```json
  {
    "status": "string",
    "database": "string"
  }
  ```
- **Example Response**:
  ```json
  {
    "status": "healthy",
    "database": "Neo4j connected"
  }
  ```
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/health"
  ```

---

## 2. Get Conversation Details

Retrieves the properties (title, status, created_at, updated_at) of a single conversation node.

- **Path**: `/graph/conversations/{conversation_id}`
- **Method**: `GET`
- **Path Parameters**:
  - `conversation_id` (string, required): The UUID or unique ID of the conversation.
- **Response Schema (`200 OK`)**:
  ```json
  {
    "conversation_id": "string",
    "user_id": "string",
    "title": "string",
    "status": "string",
    "created_at": "string",
    "updated_at": "string"
  }
  ```
- **Error Responses**:
  - `404 Not Found`: Returned if the conversation does not exist.
    ```json
    {
      "detail": "Conversation node with ID 'missing-id' not found in the graph."
    }
    ```
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/conversations/consist-root-1"
  ```

---

## 3. Get Parent Node

Retrieves the immediate parent conversation node.

- **Path**: `/graph/conversations/{conversation_id}/parent`
- **Method**: `GET`
- **Path Parameters**:
  - `conversation_id` (string, required): The ID of the conversation node to query.
- **Response Schema (`200 OK`)**:
  ```json
  {
    "conversation_id": "string",
    "user_id": "string",
    "title": "string",
    "status": "string",
    "created_at": "string",
    "updated_at": "string"
  }
  ```
- **Error Responses**:
  - `404 Not Found`: If the node itself is missing, or if it does not have a parent (e.g. it is a root conversation).
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/conversations/consist-child-1/parent"
  ```

---

## 4. Get Children Nodes (Paginated)

Retrieves immediate children nodes with pagination.

- **Path**: `/graph/conversations/{conversation_id}/children`
- **Method**: `GET`
- **Path Parameters**:
  - `conversation_id` (string, required)
- **Query Parameters**:
  - `skip` (integer, optional, default: `0`, validation: `>= 0`): Number of nodes to skip.
  - `limit` (integer, optional, default: `100`, validation: `>= 1` and `<= 1000`): Maximum number of nodes to return.
- **Response Schema (`200 OK`)**:
  ```json
  [
    {
      "conversation_id": "string",
      "user_id": "string",
      "title": "string",
      "status": "string",
      "created_at": "string",
      "updated_at": "string"
    }
  ]
  ```
- **Error Responses**:
  - `404 Not Found`: If the conversation node does not exist.
  - `422 Unprocessable Entity`: If `skip` is negative, or if `limit` is out of bounds (less than 1 or greater than 1000).
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/conversations/consist-root-1/children?skip=0&limit=5"
  ```

---

## 5. Get Ancestors (Paginated)

Retrieves all ancestors (complete linear parent chain going up to the root) with pagination.

- **Path**: `/graph/conversations/{conversation_id}/ancestors`
- **Method**: `GET`
- **Path Parameters**:
  - `conversation_id` (string, required)
- **Query Parameters**:
  - `skip` (integer, optional, default: `0`, validation: `>= 0`)
  - `limit` (integer, optional, default: `100`, validation: `>= 1` and `<= 1000`)
- **Response Schema (`200 OK`)**: Array of conversation node schemas.
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/conversations/consist-child-1/ancestors?skip=0&limit=10"
  ```

---

## 6. Get Descendants (Paginated)

Retrieves the complete descendants subtree branching down from the queried conversation node.

- **Path**: `/graph/conversations/{conversation_id}/descendants`
- **Method**: `GET`
- **Path Parameters**:
  - `conversation_id` (string, required)
- **Query Parameters**:
  - `skip` (integer, optional, default: `0`, validation: `>= 0`)
  - `limit` (integer, optional, default: `100`, validation: `>= 1` and `<= 1000`)
- **Response Schema (`200 OK`)**: Array of conversation node schemas.
- **cURL Request**:
  ```bash
  curl -X GET "http://127.0.0.1:8000/graph/conversations/consist-root-1/descendants?skip=0&limit=20"
  ```
