-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    upload_metadata.request_payer = "XYZ"

    return true, upload_metadata
end
