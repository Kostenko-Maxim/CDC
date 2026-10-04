import os
import random
import signal
import time

from .runtime import db


def generate():
    rate = float(os.environ.get("GENERATOR_RATE", "2"))
    if rate <= 0:
        raise ValueError("GENERATOR_RATE must be positive; stop the service to pause it")
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    rng = random.Random(42)
    count = 0
    print(f"Writing about {rate:g} transactions/s", flush=True)
    with db() as connection:
        while running:
            started = time.monotonic()
            action = rng.random()
            if action < 0.60:
                product = rng.randint(1, 100)
                quantity = rng.randint(1, 5)
                connection.execute(
                    "INSERT INTO shop.orders (customer_id, product_id, quantity, amount) "
                    "SELECT %s, product_id, %s, price * %s FROM shop.products "
                    "WHERE product_id = %s",
                    (rng.randint(1, 1000), quantity, quantity, product))
            elif action < 0.95:
                connection.execute(
                    "UPDATE shop.orders SET status = %s, updated_at = now() "
                    "WHERE order_id = (SELECT order_id FROM shop.orders "
                    "WHERE status IN ('new','paid','shipped') "
                    "ORDER BY random() LIMIT 1)", (rng.choice(["paid", "shipped"]),))
            else:
                connection.execute(
                    "DELETE FROM shop.orders WHERE order_id = "
                    "(SELECT order_id FROM shop.orders WHERE status IN ('new','paid','shipped') "
                    "ORDER BY random() LIMIT 1)")
            count += 1
            if count % 120 == 0:
                print(f"Committed {count} transactions", flush=True)
            time.sleep(max(0, 1 / rate - (time.monotonic() - started)))
