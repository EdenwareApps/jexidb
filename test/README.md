## Test Results
The following are the results of the automated tests conducted on my PC for different serialization formats and compression methods.

| Format                         | Size (bytes) | Time elapsed (ms) |
|-------------------------------|--------------|--------------------|
| JSON                          | 1117         | 21                 |
| V8 Serialization              | 1124         | 12 &#127942;       |
| JSON with Brotli Compression   | 1038	&#127942; | 20              |
| V8 with Brotli Compression     | 1052         | 15                |

When using V8 for serialization, be aware that serialized data may not deserialize correctly across different versions of V8 (which powers Chromium, Node.js, and Electron), leading to potential data loss or corruption. This issue is specific to V8 serialization; however, when using JSON, the data remains universal and compatible across environments.

To avoid this issue, always use your V8 database with the same version of Node.js.
