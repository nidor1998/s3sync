-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    print("upload_metadata.acl:", upload_metadata.acl)
    print("upload_metadata.cache_control:", upload_metadata.cache_control)
    print("upload_metadata.content_disposition:", upload_metadata.content_disposition)
    print("upload_metadata.content_encoding:", upload_metadata.content_encoding)
    print("upload_metadata.content_language:", upload_metadata.content_language)
    print("upload_metadata.content_type:", upload_metadata.content_type)
    print("upload_metadata.expires_string:", upload_metadata.expires_string)
    print("upload_metadata.request_payer", upload_metadata.request_payer)
    print("upload_metadata.storage_class", upload_metadata.storage_class)
    print("upload_metadata.website_redirect_location:", upload_metadata.website_redirect_location)
    print("upload_metadata.tagging:", upload_metadata.tagging)
    for key, value in pairs(upload_metadata.metadata) do
        print("upload_metadata.metadata:", key, value)
    end
    print("")

    upload_metadata.acl = nil
    upload_metadata.cache_control = nil
    upload_metadata.content_disposition = nil
    upload_metadata.content_encoding = nil
    upload_metadata.content_language = nil
    upload_metadata.content_type = nil
    upload_metadata.request_payer = nil
    upload_metadata.storage_class = nil
    upload_metadata.website_redirect_location = nil
    upload_metadata.tagging = nil
    upload_metadata.expires_string = nil
    upload_metadata.metadata = nil

    return true, upload_metadata
end
