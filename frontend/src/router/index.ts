import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import BatchReplayView from '@/views/BatchReplayView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/batch_replay', name: 'batch_replay', component: BatchReplayView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
