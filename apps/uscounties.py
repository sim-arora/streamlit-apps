import streamlit as st
import leafmap.foliumap as leafmap


def app():

    st.title("US Counties")

    filepath = "https://github.com/sim-arora/streamlit-apps/blob/main/data/georef-united-states-of-america-county.geojson"
    m = leafmap.Map(tiles="stamentoner")
    m.add_geojson(
        in_geojson=filepath
    )
    m.to_streamlit(height=700)
