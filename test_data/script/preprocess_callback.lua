-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    upload_metadata.cache_control = "s-maxage=1604800"
    upload_metadata.content_disposition = "attachment; filename=\"filename.jpg\""
    upload_metadata.content_encoding = "deflate"
    upload_metadata.content_language = "en-US,en-CA"
    upload_metadata.content_type = "application/vnd.ms-excel"
    upload_metadata.website_redirect_location = "/redirect"
    upload_metadata.tagging = "tag1=tag_value1&tag2=tag_value2"
    upload_metadata.expires_string = "2055-05-20T00:00:00.000Z"
    upload_metadata.metadata["key1"] = "value1"
    upload_metadata.metadata["key2"] = "value2"

    return true, upload_metadata
end
