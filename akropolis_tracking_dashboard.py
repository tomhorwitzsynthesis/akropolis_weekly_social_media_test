#!/usr/bin/env python3
"""
Akropolis Tracking Dashboard
Simple dashboard with links to all tracking applications
"""

import streamlit as st

def main():
    st.set_page_config(
        page_title="Akropolis Tracking Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Akropolis Tracking Dashboard")
    st.markdown("---")
    
    st.header("📈 Monthly Tracking")
    st.markdown("""
    **Akropolis Monthly Tracking Dashboard**
    
    [🔗 Open Monthly Tracking Dashboard](https://akropolismonthlytrackingtest-8pmjx2uq4btcqbpbgucqhb.streamlit.app/)
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("📱 Weekly Ads")
        st.markdown("""
        **Akropolis Weekly Ads Dashboard**
        
        [🔗 Open Weekly Ads Dashboard](https://akropolisweeklyadstest-erfnsca8fozjbyt6nrfimz.streamlit.app/)
        """)
    
    with col2:
        st.header("📊 Weekly Social Media")
        st.markdown("""
        **Akropolis Weekly Social Media Dashboard**
        
        [🔗 Open Social Media Dashboard](https://akropolisweeklysocialmediatest-twcfzbwexd5l7vcfbh4jpf.streamlit.app/)
        """)
    
    st.markdown("---")
    st.markdown("*Select any of the above links to access the respective tracking dashboard.*")

if __name__ == "__main__":
    main()
