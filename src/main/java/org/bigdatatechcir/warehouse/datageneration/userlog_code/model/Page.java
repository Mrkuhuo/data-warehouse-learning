package org.bigdatatechcir.warehouse.datageneration.userlog_code.model;

import lombok.Data;

@Data
public class Page {
    private Long during_time;
    private String item;
    private String item_type;
    private String last_page_id;
    private String page_id;
    private String source_type;
} 