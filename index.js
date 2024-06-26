#!/usr/bin/env node
import { logger, getSymbol } from './lib/logger.js'
import { options, configValid }  from './lib/args.js'
import * as CONSTANTS from './lib/consts.js'
const minApiVersion = CONSTANTS.MIN_API_VERSION
const component = 'index'
if (!configValid) {
  logger.error({ component, message: 'invalid configuration... Exiting'})
  logger.end()
  process.exit(1)
}
import startFsEventWatcher from './lib/events.js'
import * as auth from './lib/auth.js'
import * as api from './lib/api.js'
import { serializeError } from 'serialize-error'
import { initScanner } from './lib/scan.js'
import semverGte from 'semver/functions/gte.js'
import Alarm from './lib/alarm.js'



process.on('SIGINT', () => {
  logger.info({
    component,
    message: 'received SIGINT, exiting'
  })
  process.exit(0)
})

Alarm.on('shutdown', (exitCode) => {
  logger.error({
    component,
    message: `received shutdown event with code ${exitCode}, exiting`
  })
  process.exit(exitCode)
})

Alarm.on('alarmRaised', (alarmType) => {
  logger.error({
    component,
    message: `Alarm raised: ${alarmType}`
  })
})

Alarm.on('alarmLowered', (alarmType) => {
  logger.info({
    component,
    message: `Alarm lowered: ${alarmType}`
  })
})

run()

async function run() {
  try {
    logger.info({
      component,
      message: 'running',
      pid: process.pid,
      options: getObfuscatedConfig(options)
    })
    
    await preflightServices()
    setupAlarmHandlers()
    if (options.mode === 'events') {
      startFsEventWatcher()
    }
    else if (options.mode === 'scan') {
      initScanner()
    }
  }
  catch (e) {
    logError(e)
    logger.end()
    process.exitCode = CONSTANTS.ERR_FAILINIT
  }
}

function setupAlarmHandlers() {
  const alarmHandlers = {
    apiOffline: api.offlineRetryHandler,
    authOffline: auth.offlineRetryHandler,
    noGrant: () => Alarm.shutdown(CONSTANTS.ERR_NOGRANT),
    noToken: () => Alarm.shutdown(CONSTANTS.ERR_NOTOKEN)
  }
  Alarm.on('alarmRaised', (alarmType) => {
    alarmHandlers[alarmType]?.()
  })
}

function logError(e) {
  const errorObj = {
    component: e.component || 'index',
    message: e.message,
  }
  if (e.request) {
    errorObj.request = {
      method: e.request.options?.method,
      url: e.request.requestUrl,
      body: getSymbol(e.request, 'body')
    }
  }
  if (e.response) {
    errorObj.response = {
      status: e.response.statusCode,
      body: e.response.body
    }
  }
  if (e.name !== 'RequestError' && e.name !== 'HTTPError') {
    errorObj.error = serializeError(e)
  }
  logger.error(errorObj)
}

async function hasMinApiVersion () {
  const [remoteApiVersion] = await api.getDefinition('$.info.version')
  logger.info({ component, message: `preflight API version`, minApiVersion, remoteApiVersion})
  if (semverGte(remoteApiVersion, minApiVersion)) {
    return true
  }
  else {
    throw new Error(`Remote API version ${remoteApiVersion} is not compatible with this release.`)
  }
}

async function preflightServices () {
  await hasMinApiVersion()
  await auth.getOpenIDConfiguration()
  await auth.getToken()
  logger.info({ component, message: `preflight token request succeeded`})
  const promises = [
    api.getCollection(options.collectionId),
    api.getInstalledStigs(),
    api.getScapBenchmarkMap()
  ]
  await Promise.all(promises)
  setInterval(refreshCollection, 10 * 60000)
  
  // OAuth scope 'stig-manager:user:read' was not required for early versions of Watcher
  // For now, fail gracefully if we are blocked from calling /user
  try {
    await api.getUser()
    setInterval(refreshUser, 10 * 60000)
  }
  catch (e) {
    logger.warn({ component, message: `preflight user request failed; token may be missing scope 'stig-manager:user:read'? Watcher will not set {"status": "accepted"}`})
    Alarm.noGrant(false)
  }
  logger.info({ component, message: `preflight api requests succeeded`})
}

function getObfuscatedConfig (options) {
  const securedConfig = {...options}
  if (securedConfig.clientSecret) {
    securedConfig.clientSecret = '[hidden]'
  }
  return securedConfig
}

async function refreshUser() {
  try {
    if (Alarm.isAlarmed()) return
    logger.info({
      component,
      message: 'refreshing user cache'
    })
    await api.getUser()
  }
  catch (e) {
    logError(e)
  }
}

async function refreshCollection() {
  try {
    if (Alarm.isAlarmed()) return
    logger.info({
      component,
      message: 'refreshing collection cache'
    })
    await api.getCollection(options.collectionId)
  }
  catch (e) {
    logError(e)
  }
}
