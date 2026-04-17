"""
DataPulse - KPI Router
All /api/v1/kpis/* endpoints.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger("datapulse.routers.kpis")

router = APIRouter(prefix="/kpis", tags=["kpis"])


def _get_cosmos(request: Request):
    """Helper to get Cosmos client from app state."""
    return request.app.state.cosmos


@router.get("/summary")
async def get_summary(request: Request):
    """
    Overall dashboard summary: total revenue, orders, AOV, clicks.
    Used for the top-level KPI cards.
    """
    cosmos = _get_cosmos(request)
    results = await cosmos.query("summary", limit=1)

    if not results:
        raise HTTPException(status_code=404, detail="Summary KPI not yet computed. Run gold_aggregate.py first.")

    return {"status": "ok", "data": results[0]}


@router.get("/revenue")
async def get_revenue(
    request: Request,
    region: Optional[str] = Query(None, description="Filter by region name"),
    limit: int = Query(48, ge=1, le=200, description="Max rows returned"),
):
    """
    Hourly revenue breakdown by region.
    Returns the last N hourly buckets, sorted by hour descending.
    """
    cosmos   = _get_cosmos(request)
    extra    = f"c.region = '{region}'" if region else ""
    results  = await cosmos.query("hourly_revenue", extra_filter=extra, limit=limit)

    # Sort by hour_bucket descending
    results.sort(key=lambda x: x.get("hour_bucket", ""), reverse=True)

    return {
        "status": "ok",
        "count": len(results),
        "region_filter": region,
        "data": results,
    }


@router.get("/products")
async def get_top_products(
    request: Request,
    category: Optional[str] = Query(None, description="Filter by product category"),
    limit: int = Query(10, ge=1, le=50, description="Number of top products"),
):
    """
    Top products ranked by revenue.
    Optionally filtered by category.
    """
    cosmos  = _get_cosmos(request)
    extra   = f"c.category = '{category}'" if category else ""
    results = await cosmos.query("top_products", extra_filter=extra, limit=limit)

    # Sort by revenue descending
    results.sort(key=lambda x: x.get("revenue", 0), reverse=True)

    return {
        "status": "ok",
        "count": len(results),
        "category_filter": category,
        "data": results,
    }


@router.get("/conversion")
async def get_conversion(request: Request):
    """
    Conversion funnel metrics:
    - Total clicks → add-to-cart → completed orders
    - Conversion rate %, cart abandonment %, add-to-cart rate %
    """
    cosmos   = _get_cosmos(request)
    results  = await cosmos.query("conversion", limit=1)

    if not results:
        raise HTTPException(status_code=404, detail="Conversion KPI not yet computed.")

    return {"status": "ok", "data": results[0]}


@router.get("/inventory")
async def get_inventory_alerts(
    request: Request,
    alerts_only: bool = Query(True, description="If true, return only low-stock items"),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Inventory stock levels.
    By default returns only items with low_stock_alert=true.
    """
    cosmos  = _get_cosmos(request)
    extra   = "c.low_stock_alert = true" if alerts_only else ""
    results = await cosmos.query("inventory", extra_filter=extra, limit=limit)

    results.sort(key=lambda x: x.get("current_stock", 999))

    return {
        "status": "ok",
        "count": len(results),
        "alerts_only": alerts_only,
        "data": results,
    }


@router.get("/regions")
async def get_regions(request: Request):
    """List all distinct regions available in revenue data."""
    cosmos  = _get_cosmos(request)
    results = await cosmos.query("hourly_revenue", limit=200)
    regions = sorted(set(r.get("region") for r in results if r.get("region")))
    return {"status": "ok", "regions": regions}
