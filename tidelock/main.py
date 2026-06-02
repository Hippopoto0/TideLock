from __future__ import annotations

import time

import pandas as pd
from pydantic import BaseModel, ConfigDict

from tidelock.engine import Flow, cli, pipeline, step


class SharedState(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    api_meta: dict | None = None
    raw_records: list[dict] | None = None
    order_dataframe: pd.DataFrame | None = None
    routing_tag: str | None = None


@step("fetch_orders")
def fetch_orders(shared: SharedState) -> None:
    time.sleep(0.5)
    shared.api_meta = {
        "endpoint": "/v3/orders",
        "status": 200,
        "catalog": {
            "regions": ["us-east-1", "eu-west-2", "ap-southeast-1"],
            "products": [
                {
                    "sku": f"SKU-{i:04d}",
                    "name": f"Widget {i}",
                    "category": "electronics" if i % 2 == 0 else "home",
                    "in_stock": i % 5 != 0,
                    "attributes": {"weight_kg": round(0.5 + i * 0.1, 2), "color": "blue"},
                }
                for i in range(80)
            ],
            "policies": {
                "returns": {"window_days": 30, "restocking_fee": False},
                "shipping": {"free_over": 50.0, "carriers": ["UPS", "DHL"]},
            },
        },
    }
    shared.raw_records = [
        {"id": 1, "total": 450.00},
        {"id": 2, "total": 25.00},
        {"id": 3, "total": 890.00},
    ]


@step("validate_transform")
def validate_transform(shared: SharedState) -> str:
    df = pd.DataFrame(shared.raw_records)
    shared.order_dataframe = df
    return "high_volume" if df["total"].sum() > 500 else "standard_volume"


@step("route_vip_treatment")
def route_vip_treatment(shared: SharedState) -> None:
    shared.routing_tag = "Executed Enterprise Premium Workflow Logic"


@step("route_standard_treatment")
def route_standard_treatment(shared: SharedState) -> None:
    shared.routing_tag = "Executed Basic Standalone Processing Layout"


@pipeline()
def construct_business_graph() -> Flow:
    return Flow(
        fetch_orders >> validate_transform,
        (validate_transform - "high_volume") >> route_vip_treatment,
        (validate_transform - "standard_volume") >> route_standard_treatment,
        state_cls=SharedState,
    )


if __name__ == "__main__":
    cli()
