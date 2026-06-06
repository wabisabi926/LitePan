<template>
  <div class="login-container">
    <!-- 左侧：动画角色区 -->
    <div class="left-panel">
      <div class="brand-row">
        <img src="/static/img/logo.png" alt="LitePan" class="brand-logo" />
      </div>

      <div class="characters-area">
        <AnimatedCharacters
          :is-typing="isTyping"
          :show-password="showPassword"
          :password-length="passwordValue.length"
          :is-password-guard-mode="isPasswordGuardMode"
        />
      </div>

      <div class="decor-blur1" />
      <div class="decor-blur2" />
      <div class="decor-grid" />
    </div>

    <!-- 右侧：登录表单 -->
    <div class="right-panel">
      <div class="form-card">
        <p class="form-tag">管理员登录</p>
        <div class="mobile-logo">
          <span>LitePan 控制台</span>
        </div>

        <div class="form-header">
          <h1 class="form-title">欢迎回来</h1>
          <p class="form-subtitle">轻量级多网盘聚合管理系统</p>
        </div>

        <el-form
          ref="loginForm"
          :model="loginData"
          :rules="loginRules"
          class="login-form"
          @submit.prevent="handleLogin"
        >
          <div class="field-label">用户名</div>
          <el-form-item prop="username">
            <div class="input-wrapper">
              <input
                v-model="loginData.username"
                class="login-input"
                placeholder="请输入用户名"
                autocomplete="username"
                @focus="isTyping = true"
                @blur="isTyping = false"
                @keyup.enter="focusPassword"
              />
            </div>
          </el-form-item>

          <div class="field-label">密码</div>
          <el-form-item prop="password">
            <div class="input-wrapper input-password-wrapper">
              <input
                id="login-password"
                v-model="loginData.password"
                :type="showPassword ? 'text' : 'password'"
                class="login-input"
                placeholder="请输入密码"
                autocomplete="current-password"
                @focus="onPasswordFocus"
                @blur="onPasswordBlur"
                @keyup.enter="handleLogin"
                @click="onPasswordClick"
              />
              <span class="eye-toggle" @click.stop="showPassword = !showPassword">
                <svg v-if="showPassword" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                <svg v-else viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0112 20c-7 0-11-8-11-8a18.45 18.45 0 015.06-5.94M9.9 4.24A9.12 9.12 0 0112 4c7 0 11 8 11 8a18.5 18.5 0 01-2.16 3.19m-6.72-1.07a3 3 0 11-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>
              </span>
            </div>
          </el-form-item>

          <el-form-item>
            <div class="login-options">
              <el-checkbox
                v-model="loginData.remember"
                @keydown.enter.prevent="handleLogin"
              >
                保持登录（30天）
              </el-checkbox>
              <a href="#" class="forgot-link" @click.prevent="handleForgotPassword">忘记密码？</a>
            </div>
          </el-form-item>

          <el-form-item>
            <el-button
              type="primary"
              size="large"
              class="submit-btn"
              :loading="loading"
              @click="handleLogin"
            >
              {{ loading ? '登录中...' : '登录' }}
            </el-button>
          </el-form-item>
        </el-form>

        <p class="footer-hint">首次使用请查阅文档了解初始配置</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message-box/style/css'
import axios from 'axios'
import AnimatedCharacters from '../components/AnimatedCharacters.vue'

const router = useRouter()
const loginForm = ref()
const passwordInputRef = ref()
const loading = ref(false)

// 动画相关状态
const showPassword = ref(false)
const isTyping = ref(false)
const passwordFocused = ref(false)
const passwordValue = ref('')
const isPasswordGuardMode = computed(() => passwordFocused.value)

const loginData = reactive({
  username: '',
  password: '',
  remember: false
})

const loginRules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' }
  ],
  password: [
    { required: true, message: '请输入密码', trigger: 'blur' }
  ]
}

const onPasswordClick = () => {
  passwordFocused.value = true
}

const onPasswordFocus = () => {
  passwordFocused.value = true
}

const onPasswordBlur = () => {
  passwordFocused.value = false
}

const focusPassword = () => {
  const input = document.getElementById('login-password')
  if (input) input.focus()
}

// 监听密码变化，用于角色动画
watch(() => loginData.password, (val) => {
  passwordValue.value = val || ''
})

const escapeHtml = (value) => String(value || '')
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;')

const formatCountdown = (seconds) => {
  const total = Math.max(0, Math.floor(Number(seconds) || 0))
  const minutes = Math.floor(total / 60)
  const restSeconds = total % 60
  return `${String(minutes).padStart(2, '0')}:${String(restSeconds).padStart(2, '0')}`
}

const startTempPasswordCountdown = (targetId, expiresAt) => {
  const targetExpiresAt = Number(expiresAt) || 0
  if (!targetExpiresAt) return null

  const updateCountdown = () => {
    const target = document.getElementById(targetId)
    if (!target) return

    const remaining = Math.max(0, targetExpiresAt - Math.floor(Date.now() / 1000))
    target.textContent = remaining > 0
      ? `有效期剩余 ${formatCountdown(remaining)}`
      : '临时密码已过期'
    target.classList.toggle('is-expired', remaining <= 0)
  }

  updateCountdown()
  return window.setInterval(updateCountdown, 1000)
}

const handleForgotPassword = () => {
  ElMessageBox.confirm(
    `
      <div class="reset-password-confirm">
        <div class="reset-password-confirm__hero">
          <div class="reset-password-confirm__icon" aria-hidden="true">
            <span></span>
          </div>
          <div>
            <div class="reset-password-confirm__title">生成临时密码</div>
            <div class="reset-password-confirm__desc">
              系统会在容器日志中输出一枚临时管理员密码。
            </div>
          </div>
        </div>
        <div class="reset-password-confirm__facts">
          <div class="reset-password-confirm__fact">
            <span class="reset-password-confirm__mark"></span>
            <div>
              <strong>完成改密后</strong>
              <p>新密码会替换原密码，临时密码立即失效。</p>
            </div>
          </div>
          <div class="reset-password-confirm__fact">
            <span class="reset-password-confirm__mark"></span>
            <div>
              <strong>未改密前</strong>
              <p>临时密码 10 分钟内有效，且不影响原密码。</p>
            </div>
          </div>
        </div>
      </div>
    `,
    '重置密码确认',
    {
      confirmButtonText: '确定重置',
      cancelButtonText: '取消',
      customClass: 'custom-confirm-box reset-password-box',
      dangerouslyUseHTMLString: true
    }
  ).then(async () => {
    try {
      loading.value = true
      const response = await axios.post('/api/auth/reset-password')
      if (response.data.success) {
        const serverMessage = escapeHtml(response.data.message || '')
        const expiresAt = Number(response.data.data?.expires_at || 0)
        const countdownId = `temp-password-countdown-${Date.now()}`
        const countdownHtml = expiresAt
          ? `<div id="${countdownId}" class="reset-password-result__countdown">有效期剩余 --:--</div>`
          : ''
        const html = `
          <div class="reset-password-result">
            <div class="reset-password-result__hero">
              <div class="reset-password-result__icon" aria-hidden="true"></div>
              <div class="reset-password-result__body">
                <div class="reset-password-result__title-row">
                  <div class="reset-password-result__title">临时密码已生成</div>
                  ${countdownHtml}
                </div>
                ${serverMessage ? `<div class="reset-password-result__status">${serverMessage}</div>` : ''}
              </div>
            </div>
            <div class="reset-password-result__command-card">
            <div class="reset-password-result__label">宿主机终端执行(请将litepan替换为实际容器名)</div>
              <div class="reset-password-result__code"><code>docker logs litepan</code></div>
            </div>
          </div>
        `
        const alertPromise = ElMessageBox.alert(html, '重置成功', {
          confirmButtonText: '我知道了',
          customClass: 'custom-confirm-box reset-password-box',
          dangerouslyUseHTMLString: true
        })
        const countdownTimer = startTempPasswordCountdown(countdownId, expiresAt)
        alertPromise.then(() => {}, () => {}).finally(() => {
          if (countdownTimer) {
            window.clearInterval(countdownTimer)
          }
        })
      } else {
        window.appNotification.error(response.data.message || '重置失败')
      }
    } catch (error) {
      console.error('重置密码错误:', error)
      window.appNotification.error('重置失败，无法连接到服务器')
    } finally {
      loading.value = false
    }
  }).catch(() => {
    // 取消重置
  })
}

onMounted(async () => {
  try {
    const response = await axios.get('/api/auth/status')
    if (response.data.success && response.data.data.is_admin) {
      router.push('/admin')
    }
  } catch (error) {
    // 未登录，继续显示登录页面
  }
})

const handleLogin = async () => {
  if (loading.value) return
  if (!loginForm.value) return

  try {
    await loginForm.value.validate()
    loading.value = true

    const formData = new FormData()
    formData.append('username', loginData.username)
    formData.append('password', loginData.password)
    formData.append('remember', loginData.remember ? '1' : '')

    const response = await axios.post('/api/auth/login', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      }
    })

    if (response.data.success) {
      if (response.data.data?.must_change_password) {
        window.appNotification.warning('登录成功，请先修改管理员密码')
      } else {
        window.appNotification.success('登录成功')
      }
      router.push('/admin')
    } else {
      window.appNotification.error(response.data.message || '登录失败')
    }
  } catch (error) {
    console.error('登录错误:', error)
    if (error.response?.data?.message) {
      window.appNotification.error(error.response.data.message)
    } else {
      window.appNotification.error('登录失败，请检查用户名和密码')
    }
  } finally {
    loading.value = false
  }
}
</script>

<style>
.custom-confirm-box {
  border-radius: 12px;
  border: none;
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
}
.custom-confirm-box .el-message-box__header {
  padding-bottom: 10px;
}
.custom-confirm-box .el-message-box__title {
  font-weight: 600;
  color: #1e293b;
}
.custom-confirm-box .el-message-box__content {
  font-size: 14px;
  color: #475569;
  line-height: 1.6;
}
.custom-confirm-box .el-message-box__btns .el-button {
  border-radius: 8px;
  padding: 8px 16px;
}

.reset-password-box {
  width: 460px;
  max-width: calc(100vw - 32px);
  border-radius: 18px;
  overflow: hidden;
  box-shadow: 0 24px 60px rgba(15, 23, 42, 0.18);
}

.reset-password-box .el-message-box__header {
  padding: 20px 22px 4px;
}

.reset-password-box .el-message-box__title {
  font-size: 17px;
  color: #0f172a;
}

.reset-password-box .el-message-box__content {
  margin-top: 0;
  padding: 12px 22px 8px;
}

.reset-password-box .el-message-box__message {
  width: 100%;
}

.reset-password-box .el-message-box__btns {
  padding: 12px 22px 22px;
}

.reset-password-box .el-message-box__btns .el-button {
  min-width: 96px;
  font-weight: 600;
}

.reset-password-confirm,
.reset-password-result {
  color: #334155;
  line-height: 1.6;
}

.reset-password-confirm__hero,
.reset-password-result__hero {
  display: grid;
  grid-template-columns: 44px 1fr;
  gap: 12px;
  align-items: start;
  padding: 15px;
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.96)),
    #fff;
  border: 1px solid #dbeafe;
}

.reset-password-result__hero {
  align-items: center;
}

.reset-password-confirm__icon,
.reset-password-result__icon {
  width: 44px;
  height: 44px;
  border-radius: 14px;
  display: grid;
  place-items: center;
  background: linear-gradient(135deg, #1d4ed8, #38bdf8);
  box-shadow: 0 12px 22px rgba(37, 99, 235, 0.24);
  position: relative;
}

.reset-password-confirm__icon span,
.reset-password-result__icon::before {
  content: '';
  width: 18px;
  height: 18px;
  border: 2px solid #fff;
  border-radius: 50% 50% 6px 6px;
  border-bottom-width: 6px;
  display: block;
}

.reset-password-confirm__icon::after,
.reset-password-result__icon::after {
  content: '';
  width: 14px;
  height: 8px;
  border: 2px solid #fff;
  border-bottom: 0;
  border-radius: 9px 9px 0 0;
  position: absolute;
  top: 11px;
}

.reset-password-confirm__title {
  font-weight: 700;
  color: #0f172a;
  font-size: 18px;
  margin-bottom: 6px;
}

.reset-password-confirm__desc {
  color: #475569;
  margin: 0;
}

.reset-password-confirm__facts {
  display: grid;
  gap: 12px;
  margin-top: 16px;
}

.reset-password-confirm__fact {
  display: grid;
  grid-template-columns: 10px 1fr;
  gap: 10px;
  align-items: start;
  padding: 0 2px;
}

.reset-password-confirm__mark {
  width: 4px;
  height: 28px;
  border-radius: 999px;
  background: linear-gradient(180deg, #2563eb, #38bdf8);
  margin-top: 2px;
}

.reset-password-confirm__fact strong {
  display: block;
  color: #0f172a;
  font-size: 14px;
  line-height: 1.35;
}

.reset-password-confirm__fact p {
  margin: 3px 0 0;
  color: #64748b;
}

.reset-password-result__body {
  min-width: 0;
}

.reset-password-result__title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
}

.reset-password-result__title {
  font-size: 18px;
  font-weight: 800;
  color: #0f172a;
  line-height: 1.35;
}

.reset-password-result__status {
  margin-top: 6px;
  color: #475569;
}

.reset-password-result__countdown {
  display: inline-flex;
  align-items: center;
  flex: 0 0 auto;
  min-height: 22px;
  padding: 2px 9px;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 700;
}

.reset-password-result__countdown.is-expired {
  background: #fef2f2;
  color: #b91c1c;
}

.reset-password-result__command-card {
  margin-top: 16px;
  padding: 0 2px;
}

.reset-password-result__label {
  font-size: 12px;
  font-weight: 800;
  color: #1d4ed8;
  margin-bottom: 7px;
}

.reset-password-result__code {
  background: #0f172a;
  color: #e5eefb;
  border-radius: 12px;
  padding: 11px 12px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  overflow: auto;
  white-space: nowrap;
}

.reset-password-result__code code {
  color: inherit;
  font-size: 13px;
}

:root[data-theme="dark"] .reset-password-box {
  background: #181b20;
  border: 1px solid #2b3038;
  box-shadow: 0 24px 70px rgba(0, 0, 0, 0.42);
}

:root[data-theme="dark"] .reset-password-box .el-message-box__title,
:root[data-theme="dark"] .reset-password-confirm__title,
:root[data-theme="dark"] .reset-password-confirm__fact strong,
:root[data-theme="dark"] .reset-password-result__title {
  color: #e7eaf0;
}

:root[data-theme="dark"] .reset-password-box .el-message-box__content,
:root[data-theme="dark"] .reset-password-confirm,
:root[data-theme="dark"] .reset-password-result {
  color: #cbd2dc;
}

:root[data-theme="dark"] .reset-password-confirm__desc,
:root[data-theme="dark"] .reset-password-confirm__fact p,
:root[data-theme="dark"] .reset-password-result__status {
  color: #9099a8;
}

:root[data-theme="dark"] .reset-password-result__countdown {
  background: rgba(37, 99, 235, 0.18);
  color: #93c5fd;
}

:root[data-theme="dark"] .reset-password-result__countdown.is-expired {
  background: rgba(239, 68, 68, 0.14);
  color: #fca5a5;
}

:root[data-theme="dark"] .reset-password-confirm__hero,
:root[data-theme="dark"] .reset-password-result__hero {
  background:
    linear-gradient(135deg, rgba(30, 64, 175, 0.22), rgba(15, 23, 42, 0.42)),
    #1d222a;
  border-color: rgba(96, 165, 250, 0.22);
}

:root[data-theme="dark"] .reset-password-result__label {
  color: #93c5fd;
}

:root[data-theme="dark"] .reset-password-result__code {
  background: #090d14;
  color: #dbeafe;
  border: 1px solid rgba(148, 163, 184, 0.18);
}

:root[data-theme="dark"] .reset-password-box .el-message-box__btns .el-button:not(.el-button--primary) {
  background: #20252d;
  border-color: #343b46;
  color: #cbd2dc;
}

:root[data-theme="dark"] .reset-password-box .el-message-box__btns .el-button:not(.el-button--primary):hover {
  background: #262d37;
  border-color: #475569;
  color: #e7eaf0;
}

@media (max-width: 560px) {
  .reset-password-confirm__hero,
  .reset-password-result__hero {
    grid-template-columns: 1fr;
  }

  .reset-password-result__title-row {
    align-items: flex-start;
    flex-direction: column;
    gap: 6px;
  }
}
</style>

<style scoped>
.login-container {
  min-height: 100vh;
  display: grid;
  grid-template-columns: 1fr 1fr;
}

@media (max-width: 1024px) {
  .login-container {
    grid-template-columns: 1fr;
  }
}

/* ===== 左侧动画面板 ===== */
.left-panel {
  position: relative;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  padding: 48px;
  background: linear-gradient(145deg, #0f172a 0%, #1e3a8a 50%, #1e40af 100%);
  overflow: hidden;
}

@media (max-width: 1024px) {
  .left-panel {
    display: none;
  }
}

.brand-row {
  position: relative;
  z-index: 20;
  display: flex;
  align-items: center;
  gap: 10px;
}

.brand-logo {
  height: 34px;
  width: auto;
  display: block;
}

.characters-area {
  position: relative;
  z-index: 20;
  display: flex;
  align-items: flex-end;
  justify-content: center;
  height: 500px;
}

.decor-blur1 {
  position: absolute;
  top: 15%;
  right: 10%;
  width: 300px;
  height: 300px;
  background: rgba(59, 130, 246, 0.25);
  border-radius: 50%;
  filter: blur(80px);
  pointer-events: none;
  z-index: 0;
}

.decor-blur2 {
  position: absolute;
  bottom: 10%;
  left: 5%;
  width: 400px;
  height: 400px;
  background: rgba(30, 64, 175, 0.3);
  border-radius: 50%;
  filter: blur(100px);
  pointer-events: none;
  z-index: 0;
}

.decor-grid {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
  background-size: 40px 40px;
  pointer-events: none;
  z-index: 1;
}

/* ===== 右侧表单面板 ===== */
.right-panel {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px;
  background:
    linear-gradient(to right, rgba(30, 58, 138, 0.18) 0%, transparent 18%),
    radial-gradient(circle at 20% 0%, rgba(241, 245, 255, 0.9), transparent 35%),
    radial-gradient(circle at 90% 80%, rgba(219, 234, 254, 0.9), transparent 40%),
    linear-gradient(160deg, #f8fafc 0%, #eef2ff 52%, #eff6ff 100%);
}

.form-card {
  width: 100%;
  max-width: 430px;
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.86);
  border: 1px solid rgba(148, 163, 184, 0.24);
  box-shadow: 0 24px 50px rgba(30, 41, 59, 0.12);
  backdrop-filter: blur(14px);
  padding: 36px 32px 30px;
}

.form-tag {
  margin: 0 0 16px;
  text-align: center;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  color: #1e40af;
}

.mobile-logo {
  display: none;
  align-items: center;
  justify-content: center;
  gap: 8px;
  font-size: 18px;
  font-weight: 700;
  color: #0f172a;
  margin-bottom: 24px;
}

@media (max-width: 1024px) {
  .mobile-logo {
    display: flex;
  }
}

.form-header {
  text-align: center;
  margin-bottom: 28px;
}

.form-title {
  font-size: 28px;
  font-weight: 700;
  letter-spacing: -0.03em;
  color: #0b1220;
  margin: 0 0 8px 0;
  line-height: 1.3;
}

.form-subtitle {
  font-size: 14px;
  color: #64748b;
  margin: 0;
  line-height: 1.6;
}

/* ===== 表单样式 ===== */
.login-form {
  margin-bottom: 0;
}

.login-form :deep(.el-form-item) {
  margin-bottom: 20px;
}

.login-form :deep(.el-form-item__error) {
  font-size: 13px !important;
  margin-top: 4px !important;
}

.field-label {
  font-size: 13px;
  font-weight: 600;
  color: #334155;
  margin-bottom: 6px;
  letter-spacing: 0.3px;
  text-transform: uppercase;
}

/* ===== 输入框 ===== */
.input-wrapper {
  position: relative;
  width: 100%;
}

.login-input {
  width: 100%;
  height: 50px;
  padding: 0 15px;
  font-size: 14px;
  color: #111827;
  background: rgba(248, 250, 252, 0.95);
  border: 1px solid #d8dee8;
  border-radius: 14px;
  outline: none;
  box-sizing: border-box;
  transition: border-color 0.2s, box-shadow 0.2s, background 0.2s;
}

.login-input::placeholder {
  color: #9aa4b2;
}

.login-input:hover {
  border-color: #3b82f6;
  background: #ffffff;
}

.login-input:focus {
  border-color: #1e40af;
  background: #ffffff;
  box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.15);
}

.input-password-wrapper .login-input {
  padding-right: 44px;
}

.eye-toggle {
  position: absolute;
  right: 14px;
  top: 50%;
  transform: translateY(-50%);
  color: #64748b;
  cursor: pointer;
  display: flex;
  align-items: center;
  transition: color 0.2s;
  z-index: 1;
}

.eye-toggle:hover {
  color: #1e40af;
}

.login-options {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.forgot-link {
  color: #4c74df;
  font-size: 14px;
  text-decoration: none;
  transition: color 0.2s;
}

.forgot-link:hover {
  color: #1e40af;
  text-decoration: none;
}

.submit-btn {
  width: 100% !important;
  height: 52px !important;
  font-size: 15px !important;
  font-weight: 600 !important;
  border-radius: 14px !important;
  background: linear-gradient(135deg, #1e40af 0%, #4C74DF 55%, #02A6F0 100%) !important;
  border: none !important;
  letter-spacing: 0.5px;
  box-shadow: 0 14px 26px rgba(30, 64, 175, 0.24);
  transition: transform 0.2s, box-shadow 0.2s, opacity 0.2s !important;
}

.submit-btn:hover {
  transform: translateY(-1px);
  box-shadow: 0 16px 28px rgba(30, 64, 175, 0.32) !important;
}

.submit-btn:active {
  transform: translateY(1px);
}

.submit-btn.is-loading {
  background: linear-gradient(135deg, #1e40af 0%, #4C74DF 55%, #02A6F0 100%) !important;
}

.submit-btn :deep(.el-button__text) {
  color: #ffffff !important;
}

/* Override Element Plus primary button default blue */
.submit-btn.el-button--primary {
  --el-button-bg-color: transparent;
  --el-button-border-color: transparent;
  --el-button-hover-bg-color: transparent;
  --el-button-hover-border-color: transparent;
  --el-button-active-bg-color: transparent;
  --el-button-active-border-color: transparent;
}

.footer-hint {
  text-align: center;
  font-size: 12px;
  color: #64748b;
  margin: 20px 6px 0;
  line-height: 1.6;
}
</style>
