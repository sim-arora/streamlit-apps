import streamlit as st
import folium
import geopandas as gpd
from shapely.geometry import LineString

#Functions

def main():
    st.title("US Counties Selector")
    
    county_gdf = gpd.read_file(file_path)

    m = folium.Map(location=[41.00792926996004, -97.76132662516906], zoom_start=8)

    folium.GeoJson(county_gdf).add_to(m)

    drawn_line = m.drawn_line()
    if drawn_line is not None:

        line_coords = [(p.y, p.x) for p in drawn_line]
        line = LineString(line_coords)

        intersecting_counties = county_gdf[county_gdf.intersects(line)]

        st.subheader("Intersecting Counties:")
        st.write(intersecting_counties)

        highlight_layer = folium.GeoJson(intersecting_counties)
        highlight_layer.add_to(m)

    st.folium_static(m)

file_path = "https://github.com/sim-arora/streamlit-apps/blob/main/data/georef-united-states-of-america-county.geojson"

if __name__ == '__main__':
    main()
