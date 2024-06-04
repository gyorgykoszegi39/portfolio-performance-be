from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import matplotlib
#from controllers.portfolio_controller import router as portfolio_router

app = FastAPI()
matplotlib.use('Agg')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Good luck!": "Have a nice day :)"}

#app.include_router(portfolio_router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
