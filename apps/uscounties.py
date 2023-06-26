import streamlit as st
import folium
from streamlit_folium import folium_static
import geopandas as gpd
from shapely.geometry import Polygon, LineString

# functions below

def draw_from_file(filepath):
    gdf = gpd.read_file(filepath)
    m = folium.Map(location=[48.771, -94.90], zoom_start=4)
    st.map(gdf)

    #for _, row in gdf.iterrows():
        #polygon = row['geometry']
        #coordinates = list(polygons.exterior.coords)
        #folium.Polygon(locations=coordinates).add_to(m)

    drawn_line = st.map.draw_polyline()
    if drawn_line is not None:
        line = LineString(drawn_line)

        selected_counties = gdf[gdf.intersects(line)]

        st.subheader("Selected Counties")
        st.write(selected_counties)

    folium_static(m)


file_path = "https://github.com/sim-arora/streamlit-apps/blob/main/data/georef-united-states-of-america-county.geojson"

st.title("US Counties Selector")

draw_from_file(file_path)

