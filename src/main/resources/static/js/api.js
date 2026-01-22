const API_BASE_URL = window.location.origin;

function getToken() {
    return localStorage.getItem('jwt_token');
}

function clearToken() {
    localStorage.removeItem('jwt_token');
    localStorage.removeItem('user_name');
}

function decodeJWT(token) {
    try {
        const base64Url = token.split('.')[1];
        const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
        const jsonPayload = decodeURIComponent(atob(base64).split('').map((c) => {
            return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
        }).join(''));
        return JSON.parse(jsonPayload);
    } catch (error) {
        console.error('Error decoding JWT:', error);
        return null;
    }
}

function getDisplayName() {
    const token = getToken();
    if (!token) {
        return null;
    }

    const storedName = localStorage.getItem('user_name');
    const decoded = decodeJWT(token);
    return storedName || decoded?.name || decoded?.sub || null;
}

function updateWelcomeMessage(elementId = 'welcomeMessage') {
    const displayName = getDisplayName();
    const welcomeMessage = document.getElementById(elementId);

    if (welcomeMessage && displayName) {
        welcomeMessage.textContent = `Welcome, ${displayName}!`;
    }
}

function handleUnauthorized() {
    clearToken();
    window.location.href = '/';
}

async function apiRequest(endpoint, options = {}) {
    const token = getToken();
    
    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };
    
    if (token && !options.skipAuth) {
        headers['Authorization'] = `Bearer ${token}`;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, {
            ...options,
            headers
        });
        
        if (response.status === 401) {
            handleUnauthorized();
            throw new Error('Unauthorized');
        }
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `HTTP ${response.status}`);
        }
        
        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        }
        
        return await response.text();
    } catch (error) {
        if (error.message !== 'Unauthorized') {
            console.error('API request failed:', error);
        }
        throw error;
    }
}

const api = {
    get: (endpoint) => apiRequest(endpoint, { method: 'GET' }),

    post: (endpoint, data) => apiRequest(endpoint, {
        method: 'POST',
        body: JSON.stringify(data)
    }),

    postMultipart: async (endpoint, formData) => {
        const token = getToken();
        const headers = {};
        if (token) {
            headers['Authorization'] = `Bearer ${token}`;
        }

        const response = await fetch(`${API_BASE_URL}${endpoint}`, {
            method: 'POST',
            body: formData,
            headers
        });

        if (response.status === 401) {
            handleUnauthorized();
            throw new Error('Unauthorized');
        }

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `HTTP ${response.status}`);
        }

        return response.json();
    },

    put: (endpoint, data) => apiRequest(endpoint, {
        method: 'PUT',
        body: JSON.stringify(data)
    }),
    
    delete: (endpoint, data) => apiRequest(endpoint, {
        method: 'DELETE',
        body: data ? JSON.stringify(data) : undefined
    }),
    
    download: async (endpoint) => {
        const token = getToken();
        const headers = {};
        
        if (token) {
            headers['Authorization'] = `Bearer ${token}`;
        }
        
        const response = await fetch(`${API_BASE_URL}${endpoint}`, { headers });
        
        if (response.status === 401) {
            handleUnauthorized();
            throw new Error('Unauthorized');
        }
        
        if (!response.ok) {
            throw new Error(`Download failed: ${response.status}`);
        }
        
        return response.blob();
    }
};

function requireAuth() {
    const token = getToken();
    if (!token) {
        window.location.href = '/';
        return false;
    }
    return true;
}
