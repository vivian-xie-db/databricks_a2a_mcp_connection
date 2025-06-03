from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.utils import new_agent_text_message
from genie_room import genie_query
import pandas as pd
import json

class GenieAgent:
    """Hello World Agent."""

    async def invoke(self,message: str) -> str:
        response, _ = await genie_query(message)
        if isinstance(response, pd.DataFrame):
            # turn pandas dataframe into a json
            response = response.to_json(orient="records")
        return response


class GenieAgentExecutor(AgentExecutor):
    """Test AgentProxy Implementation."""

    def __init__(self):
        self.agent = GenieAgent()

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        print("context--",context.get_user_input())
        result = await self.agent.invoke(context.get_user_input())
        event_queue.enqueue_event(new_agent_text_message(result))

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        raise Exception('cancel not supported')
