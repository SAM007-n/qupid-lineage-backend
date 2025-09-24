package com.lineage.dto;

import java.util.List;

public class SettingsDtos {

    public static class GroqKeyRequest {
        private String groqKey;
        
        public String getGroqKey() { 
            return groqKey; 
        }
        
        public void setGroqKey(String groqKey) { 
            this.groqKey = groqKey; 
        }
    }

    public static class GroqKeyResponse {
        private String groqKeyMasked;
        private boolean isSet;
        
        public String getGroqKeyMasked() { 
            return groqKeyMasked; 
        }
        
        public void setGroqKeyMasked(String groqKeyMasked) { 
            this.groqKeyMasked = groqKeyMasked; 
        }
        
        public boolean isSet() { 
            return isSet; 
        }
        
        public void setSet(boolean isSet) { 
            this.isSet = isSet; 
        }
    }

    public static class GroqModel {
        private String id;
        private String object;
        private long created;
        private String ownedBy;
        
        public String getId() { 
            return id; 
        }
        
        public void setId(String id) { 
            this.id = id; 
        }
        
        public String getObject() { 
            return object; 
        }
        
        public void setObject(String object) { 
            this.object = object; 
        }
        
        public long getCreated() { 
            return created; 
        }
        
        public void setCreated(long created) { 
            this.created = created; 
        }
        
        public String getOwnedBy() { 
            return ownedBy; 
        }
        
        public void setOwnedBy(String ownedBy) { 
            this.ownedBy = ownedBy; 
        }
    }

    public static class GroqModelsResponse {
        private String object;
        private List<GroqModel> data;
        
        public String getObject() { 
            return object; 
        }
        
        public void setObject(String object) { 
            this.object = object; 
        }
        
        public List<GroqModel> getData() { 
            return data; 
        }
        
        public void setData(List<GroqModel> data) { 
            this.data = data; 
        }
    }

    public static class ApiErrorResponse {
        private String error;
        private String message;
        
        public String getError() { 
            return error; 
        }
        
        public void setError(String error) { 
            this.error = error; 
        }
        
        public String getMessage() { 
            return message; 
        }
        
        public void setMessage(String message) { 
            this.message = message; 
        }
    }
}
