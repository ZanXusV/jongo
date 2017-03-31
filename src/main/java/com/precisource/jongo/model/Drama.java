package com.precisource.jongo.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.types.ObjectId;
import org.jongo.marshall.jackson.oid.MongoId;

/**
 * @author zanxus
 * @version 1.0.0
 * @date 2017-03-28 下午5:19
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Drama {

    @MongoId
    @JsonIgnore
    public ObjectId id;

    @JsonProperty("href")
    public String href;

    @JsonProperty("title")
    public String title;

    @JsonProperty("text")
    public String text;

    @JsonProperty("day")
    public String day;

    @Override
    public String toString() {
        return "Drama{" +
                "id=" + id +
                ", href='" + href + '\'' +
                ", title='" + title + '\'' +
                ", text='" + text + '\'' +
                ", day='" + day + '\'' +
                '}';
    }
}
