# Great Debate

Great Debate is a debate-centered social prototype built around daily prompts, side selection, private chat rooms, and post-match AI-style judging.

## What is implemented

- Daily topic system with one active prompt per day
- Simple user registration and local identity persistence
- Side selection and queue-based matchmaking against an opposing stance
- Private debate room chat with message history and participant status
- Debate ending logic for inactivity, topic expiry, and mutual leave
- Post-debate judge pipeline with 5 persona votes, winner selection, and summary
- Elo-inspired rating updates and tier labels
- Leaderboard and simple analytics views
- File-backed SQLite persistence for users, topics, debates, messages, and ratings
- Backend API and frontend integration for end-to-end flow

## What is mocked or scaffolded

- The AI judging pipeline is currently local and heuristic-based. It is designed so real API judging can be added later without changing the main app flow.
- No production authentication is implemented; users are identified by a simple handle stored in browser local storage.
- Background scheduling is approximated by cleanup logic on API requests rather than a separate cron worker.

## Run locally

1. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

2. Start the backend server

```bash
python3 backend/app.py
```

3. Open the app in your browser

Visit: `http://localhost:5000`

## Run with Docker

Build and run the container locally:

```bash
docker build -t great-debate .
docker run -p 5000:5000 --env OPENAI_API_KEY=$OPENAI_API_KEY great-debate
```

## Deployment

This repo includes a `Dockerfile` and `Procfile` for web hosting platforms that support Python and containers.

- `Dockerfile` runs the Flask app with Gunicorn on port `5000`
- `Procfile` supports platforms like Heroku and similar container-based deploys

## Configuration

Rename `.env.example` to `.env` if you want to keep environment settings local.

- `OPENAI_API_KEY` is required for full AI judge functionality on the web.
- `OPENAI_MODEL` may be set to a preferred OpenAI model, such as `gpt-3.5-turbo`.
- `FLASK_ENV=development` is included for local development.

> Note: I cannot provide actual API keys. To make the deployment fully functional, obtain an OpenAI API key and set it in the hosting environment or local `.env` file.

## Files changed or added

- `backend/app.py` — Flask backend with API routes, SQLite persistence, matchmaking, chat, debate scoring, and judge logic
- `frontend/index.html` — Updated UI for login, daily topic, queue flow, live chat room, and results
- `frontend/app.js` — Frontend app logic and API integration
- `requirements.txt` — Backend dependency list
- `.env.example` — Environment variable template
- `README.md` — Project setup and implementation notes

## Notes for later extension

- Add OpenAI or another language model integration inside `backend/app.py` in `judge_transcript()`.
- Replace local user identity with real auth if a larger production flow is needed.
- Add a scheduler or periodic task to close stale debates without relying on API traffic.
- Add topic history pages and a richer leaderboard with daily winners.
