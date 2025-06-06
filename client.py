import logging  # Import the logging module
from typing import Any
from uuid import uuid4

import httpx

from a2a.client import A2ACardResolver, A2AClient
from a2a.types import (AgentCard, MessageSendParams, SendMessageRequest,
                       SendStreamingMessageRequest)
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
import asyncio
from databricks.sdk.core import Config
import json
config = Config(profile="e2-demo-field-eng")
token = config.oauth_token().access_token
headers = {"Authorization": f"Bearer {token}"}
url = "https://genie-app-vivian-1444828305810485.aws.databricksapps.com/api/mcp/"
transport = StreamableHttpTransport(
    url=url,
    headers=headers
)
client = Client(transport)
async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)  # Get a logger instance

    async with client:
        result = await client.list_resources()
        logger.info(result)
        response = await client.read_resource("resource://agent_cards/genie_supply_chain_agent")
        data = json.loads(response[0].text)
        agent_card = AgentCard(**data)
        logger.info(agent_card)
       
    
    base_url = 'https://a2a-server-1444828305810485.aws.databricksapps.com/api'

    async with httpx.AsyncClient(timeout=100) as httpx_client:
        # Initialize A2ACardResolver
        resolver = A2ACardResolver(
            httpx_client=httpx_client,
            base_url=base_url,
        )

        client_a2a = A2AClient(
            httpx_client=httpx_client, agent_card=agent_card
        )

        logger.info("A2AClient initialized.")

        send_message_payload: dict[str, Any] = {
            'message': {
                'role': 'user',
                'parts': [
                    {'kind': 'text', 'text': 'List top 3 distribution centers.'}
                ],
                'messageId': uuid4().hex,
            },
        }
        request = SendMessageRequest(
            params=MessageSendParams(**send_message_payload)
        )

        response = await client_a2a.send_message(request,http_kwargs={"headers": headers})
        print(response.model_dump(mode='json', exclude_none=True))
       
        streaming_request = SendStreamingMessageRequest(
                params=MessageSendParams(**send_message_payload)
        )

        stream_response = client_a2a.send_message_streaming(streaming_request,http_kwargs={"headers": headers})
        
        async for chunk in stream_response:
            print(chunk.model_dump(mode='json', exclude_none=True))
        


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
