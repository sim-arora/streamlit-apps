import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

df = gpd.read_file("C:/Users/aroras4/Desktop/Shapefiles/georef-united-states-of-america-county.geojson")

def app():

    st.title("US Counties Selector")

    filepath = "https://raw.githubusercontent.com/giswqs/leafmap/master/examples/data/us_cities.csv"
    m = folium.Map(width=1000, height=1000, location=[30.70, -93.94], zoom_start=4, tiles='CartoDB positron')

    for _, r in df.iterrows():
    sim_geo = gpd.GeoSeries(r['geometry']).simplify(tolerance=0.001)
    geo_j = sim_geo.to_json()
    geo_j = folium.GeoJson(data=geo_j
                           ,style_function=lambda x: {'fillColor': '#ffff00','color': 'black','weight': 0.25})
    folium.Popup(r['coty_gnis_code']).add_to(geo_j)
    geo_j.add_to(m)

m.to_streamlit(height=700)