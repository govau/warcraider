pub fn on_blacklist(hostname: &str, url: &str) -> bool {
    let hostname_blacklist = [
        "insolvencynotices.asic.gov.au",
        "data.gov.au",
        "trove.nla.gov.au",
        "data.aad.gov.au",
        "www.trove.nla.gov.au",
        "epubs.aims.gov.au",
        "services.aad.gov.au",
        "results.aec.gov.au",
        "periodicdisclosures.aec.gov.au",
        "transcribe.naa.gov.au",
        "bookshop.nla.gov.au",
        "recordsearch.naa.gov.au",
        "library.nma.gov.au",
        "abr.business.gov.au",
        "collections.anmm.gov.au",
        "elibrary.gbrmpa.gov.au",
        "channelfinder.acma.gov.au",
        "vrroom.naa.gov.au",
        "www.tenders.gov.au",
        "dmzapp17p.ris.environment.gov.au",
        "discoveringanzacs.naa.gov.au",
        "elibrary.gbrmpa.gov.au",
        "neats.nopta.gov.au",
        "results.aec.gov.au",
        "recordsearch.naa.gov.au",
        "services.aad.gov.au",
        "soda.naa.gov.au",
        "stat.data.abs.gov.au",
        "store.anmm.gov.au",
        "toiletmap.gov.au",
        "training.gov.au",
        "transcribe.naa.gov.au",
        "wels.agriculture.gov.au",
        "www.padil.gov.au",
        "www.screenaustralia.gov.au",
        "edit.acnc.gov.au",
        "ifp.mychild.gov.au",
        "printsandprintmaking.gov.au",
        "link.aiatsis.gov.au",
    ];
    let url_blacklist = ["http://www.nepc.gov.au/system/files/resources/45fee0f3-1266-a944-91d7-3b98439de8f8/files/dve-prepwk-project2-1-diesel-complex-cuedc.xls" ,
                     "https://www.ncver.edu.au/__data/assets/word_doc/0013/3046/2221s.doc" ,
                     "https://www.acma.gov.au/-/media/Broadcast-Carriage-Policy/Information/Word-document/reg_qld-planning_data-docx.docx?la=en",
                     "https://www.acma.gov.au/-/media/Broadcasting-Spectrum-Planning/Information/Word-Document-Digital-TV/Planning-data-Regional-Queensland-TV1.docx?la=en" ,
                     "https://beta.dva.gov.au/sites/default/files/files/providers/vendor/medvendor1sept2015.xls" ,
                     "https://www.ppsr.gov.au/sites/g/files/net3626/f/B2G%20Interface%20Specification%20R4.doc" ,
                     "http://guides.dss.gov.au/sites/default/files/2003_ABSTUDY_Policy_Manual.docx",
                     "http://www.nepc.gov.au/system/files/resources/45fee0f3-1266-a944-91d7-3b98439de8f8/files/dve-prepwk-project2-1-diesel-complex-simp-cuedc.xls"];
    
    hostname_blacklist.contains(&hostname)
        || url_blacklist.contains(&url)
        || url.matches("ca91-4-xd").count() > 0
        || url.matches("sbs.com.au/ondemand").count() > 0
        || url.matches("sbs.com.au/news").count() > 0
        || url.matches("abc.net.au/news").count() > 0
}
