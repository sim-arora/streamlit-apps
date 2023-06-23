import streamlit as st
import folium
from streamlit_folium import folium_static
import geopandas as gpd
from shapely.geometry import Polygon

# functions below

def draw_from_file(filepath)
    gdf = gpd.read_file(filepath)
    m = folium.Map(location=[48.771, -94.90], zoom_start=4)

    for _, row in gdf.iterrows():
        polygon = row['geometry']
        coordinates = list(polygons.exterior.coords)
        folium.Polygon(locations=coordinates).add_to(m)

    folium_static(m)

file_path = "C:/Users/aroras4/Desktop/Shapefiles/georef-united-states-of-america-county.geojson"

st.title("US Counties Selector")
draw_from_file(file_path)