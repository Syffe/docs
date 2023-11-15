import asyncio

import httpx
import starlette.types
import starsessions

from mergify_engine.web.front import sessions


async def test_redis_concurrent_sessions() -> None:
    async def app(
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ) -> None:
        connection = starlette.requests.HTTPConnection(scope, receive)
        await starsessions.session.load_session(connection)

        if connection.url.path == "/login":
            connection.session["logged_user"] = "joe"

        elif connection.url.path == "/logout":
            # just ensure a concurrent request can start
            await asyncio.sleep(0.2)
            connection.session.clear()

        elif connection.url.path == "/slow":
            # just ensure we are slower than the logout method
            await asyncio.sleep(0.4)

        response = starlette.responses.Response(
            connection.session.get("logged_user", "<not-logged>"),
        )
        await response(scope, receive, send)

    redis_store = sessions.RedisStore()
    await redis_store.remove("session_id")

    app = starsessions.SessionMiddleware(app, store=redis_store, lifetime=1000)

    client = httpx.AsyncClient(base_url="http://localhost:80", app=app)

    response = await client.get("/")
    assert response.content == b"<not-logged>"
    session_id = response.cookies.get("session")
    assert session_id is None

    response = await client.get("/login")
    assert response.content == b"joe"
    session_id = response.cookies.get("session")
    assert session_id is not None

    response = await client.get("/", cookies={"session": session_id})
    assert response.content == b"joe"
    assert session_id == response.cookies.get("session")

    # This take 1 second
    task_logout = asyncio.create_task(
        client.get("/logout", cookies={"session": session_id}),
    )

    # Start another slow request in the meantime
    await asyncio.sleep(0.1)
    task_slow = asyncio.create_task(
        client.get("/slow", cookies={"session": session_id}),
    )

    # Wait both finished
    await asyncio.gather(task_logout, task_slow)

    # Logout works
    response_logout = task_logout.result()
    assert response_logout.content == b"<not-logged>"
    assert session_id == response.cookies.get("session")

    # Slow start before logout finish, so it's ok to get the logged user
    response_slow = task_slow.result()
    assert response_slow.content == b"joe"
    assert session_id == response.cookies.get("session")

    # Since we logout the session should have vanished
    response = await client.get("/", cookies={"session": session_id})
    assert response.content == b"<not-logged>"
