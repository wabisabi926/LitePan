from typing import Dict, List

from core.registry import driver_registry

from .methods import TRANSFER_METHODS


def _pan_meta(info: Dict) -> Dict:
    return {
        "driver": info.get("name"),
        "name": info.get("display_name") or info.get("card_name") or info.get("name"),
        "logo": info.get("card_logo") or "",
        "color": info.get("card_color") or "",
        "conflict_policies": list(info.get("upload_conflict_policies") or ["rename", "overwrite"]),
    }


def _feasible_methods(src_info: Dict, dst_info: Dict) -> List[str]:
    provides = set(src_info.get("provide_hashes") or [])
    accepts = set(dst_info.get("rapid_upload") or [])
    return [mid for mid in TRANSFER_METHODS if mid in provides and mid in accepts]


def build_routes() -> List[Dict]:
    infos = driver_registry.get_all_driver_info()
    names = list(infos.keys())

    feasible: Dict[tuple, List[str]] = {}
    for a in names:
        for b in names:
            if a == b:
                continue
            methods = _feasible_methods(infos[a], infos[b])
            if methods:
                feasible[(a, b)] = methods

    routes: List[Dict] = []
    seen = set()
    for (a, b), methods in feasible.items():
        if (a, b) in seen:
            continue
        bidirectional = (b, a) in feasible
        method_id = methods[0]
        routes.append({
            "id": f"{a}__{b}",
            "from": _pan_meta(infos[a]),
            "to": _pan_meta(infos[b]),
            "method": method_id,
            "method_label": TRANSFER_METHODS[method_id].label,
            "bidirectional": bidirectional,
        })
        seen.add((a, b))
        if bidirectional:
            seen.add((b, a))

    return routes
