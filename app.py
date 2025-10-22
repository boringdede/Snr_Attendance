from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

@app.get("/")
async def home():
    return HTMLResponse("""
    <html>
        <head><title>SNR Attendance</title></head>
        <body style="font-family: Arial; text-align:center; margin-top:50px;">
            <h1>✅ Hello SNR Teacher</h1>
            <p>Mini-App Web Server is working!</p>
        </body>
    </html>
    """)
