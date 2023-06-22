import streamlit as st
import leafmap.foliumap as leafmap


def app():

    st.title("US Counties")

    filepath = "https://github.com/sim-arora/streamlit-apps/blob/main/data/georef-united-states-of-america-county.geojson"
    m = leafmap.Map(center=[40, -100], zoom=4)
    m.add_geojson(
        in_geojson=filepath
    )
    m.to_streamlit(height=700)
