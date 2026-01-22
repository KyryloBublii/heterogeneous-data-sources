// API Configuration
const API_BASE_URL = '';

// Utility Functions
function showMessage(elementId, message, type) {
    const messageEl = document.getElementById(elementId);
    if (messageEl) {
        messageEl.textContent = message;
        messageEl.className = `message ${type}`;
    }
}

function hideMessage(elementId) {
    const messageEl = document.getElementById(elementId);
    if (messageEl) {
        messageEl.style.display = 'none';
    }
}

function saveToken(token) {
    localStorage.setItem('jwt_token', token);
}

function saveUserName(name) {
    localStorage.setItem('user_name', name);
}

function getToken() {
    return localStorage.getItem('jwt_token');
}

function clearToken() {
    localStorage.removeItem('jwt_token');
    localStorage.removeItem('user_name');
}

function isAuthenticated() {
    return getToken() !== null;
}

// Registration Handler
if (document.getElementById('registerForm')) {
    document.getElementById('registerForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const nameField = document.getElementById('registerName') || document.getElementById('name');
        const usernameField = document.getElementById('registerEmail') || document.getElementById('username');
        const passwordField = document.getElementById('registerPassword') || document.getElementById('password');
        const confirmPasswordField = document.getElementById('confirmPassword');
        const messageId = document.getElementById('registerMessage') ? 'registerMessage' : 'message';

        const name = nameField.value.trim();
        const username = usernameField.value.trim();
        const password = passwordField.value;
        const confirmPassword = confirmPasswordField.value;

        // Validation
        if (name.length < 2) {
            showMessage(messageId, 'Please enter your name', 'error');
            return;
        }

        if (username.length < 3) {
            showMessage(messageId, 'Email must be at least 3 characters long', 'error');
            return;
        }

        if (password.length < 6) {
            showMessage(messageId, 'Password must be at least 6 characters long', 'error');
            return;
        }

        if (password !== confirmPassword) {
            showMessage(messageId, 'Passwords do not match', 'error');
            return;
        }

        // Disable submit button
        const submitBtn = e.target.querySelector('button[type="submit"]');
        const originalText = submitBtn.textContent;
        submitBtn.disabled = true;
        submitBtn.textContent = 'Registering...';

        try {
            const response = await fetch(`${API_BASE_URL}/auth/register`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ name, username, password })
            });

            const data = await response.json();

            if (response.ok) {
                showMessage(messageId, 'Registration successful! Redirecting...', 'success');
                setTimeout(() => {
                    // Auto-login after registration
                    if (document.getElementById('loginEmail')) {
                        // On single-page auth, switch to login
                        showLogin();
                        document.getElementById('loginEmail').value = username;
                    } else {
                        // On separate pages, redirect
                        window.location.href = '/';
                    }
                }, 1500);
            } else {
                showMessage(messageId, data.message || 'Registration failed', 'error');
                submitBtn.disabled = false;
                submitBtn.textContent = originalText;
            }
        } catch (error) {
            console.error('Registration error:', error);
            showMessage(messageId, 'Network error', 'error');
            submitBtn.disabled = false;
            submitBtn.textContent = originalText;
        }
    });
}

// Login Handler
if (document.getElementById('loginForm')) {
    document.getElementById('loginForm').addEventListener('submit', async (e) => {
        e.preventDefault();

        const usernameField = document.getElementById('loginEmail') || document.getElementById('username');
        const passwordField = document.getElementById('loginPassword') || document.getElementById('password');
        const messageId = document.getElementById('loginMessage') ? 'loginMessage' : 'message';

        const username = usernameField.value.trim();
        const password = passwordField.value;

        // Validation
        if (!username || !password) {
            showMessage(messageId, 'Please enter both email and password', 'error');
            return;
        }

        // Disable submit button
        const submitBtn = e.target.querySelector('button[type="submit"]');
        const originalText = submitBtn.textContent;
        submitBtn.disabled = true;
        submitBtn.textContent = 'Logging in...';

        try {
            const response = await fetch(`${API_BASE_URL}/auth/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ username, password })
            });

            if (response.ok) {
                const data = await response.json();

                if (data.token) {
                    saveToken(data.token);
                    if (data.name) {
                        saveUserName(data.name);
                    }
                    showMessage(messageId, 'Login successful! Redirecting...', 'success');
                    setTimeout(() => {
                        window.location.href = '/dashboard';
                    }, 1000);
                } else {
                    showMessage(messageId, 'Invalid response from server', 'error');
                    submitBtn.disabled = false;
                    submitBtn.textContent = originalText;
                }
            } else {
                showMessage(messageId, 'Invalid username or password', 'error');
                submitBtn.disabled = false;
                submitBtn.textContent = originalText;
            }
        } catch (error) {
            console.error('Login error:', error);
            showMessage(messageId, 'Network error', 'error');
            submitBtn.disabled = false;
            submitBtn.textContent = originalText;
        }
    });
}

// Check if already logged in (for login/register pages)
if ((window.location.pathname === '/' || window.location.pathname === '/index.html') && isAuthenticated()) {
    window.location.href = '/dashboard';
}
