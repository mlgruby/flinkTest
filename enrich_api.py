from fastapi import FastAPI, Query, BackgroundTasks
import random
import string
import asyncio

app = FastAPI()

def enrich_data(user_id: str) -> str:
    return ''.join(random.choice(string.ascii_lowercase + ' ') for _ in range(10))

@app.get("/enrich", response_model=str)
def enrich_sync(user_id: str = Query(default="unknown")):
    """Synchronous endpoint for data enrichment"""
    return enrich_data(user_id)

@app.get("/enrich_async", response_model=str)
async def enrich_async(background_tasks: BackgroundTasks, user_id: str = Query(default="unknown")):
    """Asynchronous endpoint for data enrichment"""
    async def async_enrich():
        await asyncio.sleep(2)  # Simulate a time-consuming operation
        return enrich_data(user_id)

    background_tasks.add_task(async_enrich)
    return {"message": f"Enrichment for user {user_id} started. Results will be available soon."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)