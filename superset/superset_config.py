import os


SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "shopflow-dev-superset-secret-change-me",
)

APP_NAME = "ShopFlow BI"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"
LOGO_TARGET_PATH = "/superset/welcome/"
LOGO_TOOLTIP = "ShopFlow BI"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True

# Matches the React dashboard palette in frontend/src/App.css.
SHOPFLOW_COLORS = [
    "#7fad91",  # mint
    "#b8a9d9",  # lavender
    "#a63a54",  # berry
    "#ef7f82",  # salmon
    "#c9a9c0",  # mauve
    "#44364f",  # plum
    "#e8748a",  # primary rose
    "#34293f",  # dark plum
]

EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "shopflow_react_palette",
        "description": "ShopFlow React dashboard colors.",
        "label": "ShopFlow React Palette",
        "isDefault": True,
        "colors": SHOPFLOW_COLORS,
    }
]

EXTRA_SEQUENTIAL_COLOR_SCHEMES = [
    {
        "id": "shopflow_rose_scale",
        "description": "ShopFlow rose scale for heatmaps and sequential charts.",
        "label": "ShopFlow Rose Scale",
        "isDefault": True,
        "colors": [
            "#fff7fc",
            "#f9dae2",
            "#f6c4cf",
            "#ed9cac",
            "#e8748a",
            "#a63a54",
        ],
    }
]

THEME_OVERRIDES = {
    "borderRadius": 6,
    "colors": {
        "primary": {
            "base": "#e8748a",
            "dark1": "#a63a54",
            "dark2": "#44364f",
            "light1": "#f4c3d0",
            "light2": "#fff0f7",
            "light3": "#fff7fc",
        },
        "secondary": {
            "base": "#7fad91",
            "dark1": "#4c9475",
            "light1": "#e4f4ed",
        },
        "grayscale": {
            "base": "#645866",
            "dark1": "#24202d",
            "dark2": "#2d2437",
            "light1": "#f0dce7",
            "light2": "#f7ecfb",
            "light3": "#fff7fc",
        },
    },
    "typography": {
        "families": {
            "sansSerif": "'Inter', 'Segoe UI', Arial, sans-serif",
        },
    },
}
