import streamlit as st
import geopandas as gpd
from shapely.geometry import LineString

def main():
    st.title("US Counties Selector")
    st.set_option('deprecation.showfileUploaderEncoding', False)

    uploaded_file = st.file_uploader("Upload GeoJSON file", type="geojson")
    if uploaded_file is not None:
        
        gdf = gpd.read_file(uploaded_file)

        st.map(gdf)
               
        drawn_line = st.map.drawn_polyon()
        if drawn_line is not None:
            line = LineString(drawn_line)

        selected_counties = gdf[gdf.intersects(line)]

        st.subheader("Selected Counties:")
        st.write(selected_counties)

main()