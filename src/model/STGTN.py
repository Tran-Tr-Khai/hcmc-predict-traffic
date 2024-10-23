import dgl
import dgl.nn as dgl_nn
import dgl.function as dgl_func
import torch
import torch.nn as nn
import torch.nn.functional as F


class MultiHeadAttention(nn.Module):
    def __init__(self, in_dim, dim, num_head, bias):
        super().__init__()
        assert dim % num_head == 0, "Unexpected embedding dimension"

        self.head_dim = dim // num_head
        self.num_head = num_head
        self.scale = 1./(dim ** (0.5))

        self.Wq_c = nn.Linear(in_dim, dim, bias=bias) # out = 16
        self.Wq_p = nn.Linear(in_dim, dim, bias = bias)
        self.Wk = nn.Linear(in_dim, dim, bias=bias)
        self.Wv = nn.Linear(in_dim, dim, bias=bias)

    def _reshape_qkv(self, q_c, q_p, k, v):
        '''
            q, k, v: (N, D_h * N_h)
            return: (N, N_h, D_h)
        '''
        q_c = q_c.view(-1, self.num_head, self.head_dim)
        q_p = q_p.view(-1, self.num_head, self.head_dim)
        k = k.view(-1, self.num_head, self.head_dim)
        v = v.view(-1, self.num_head, self.head_dim)
        return q_c, q_p, k, v

    def query_by_current_state(self, edges):
        Q, K = edges.dst['Q_c'], edges.src['K']
        score = (Q * K).sum(-1, keepdim = True)
        score = score * self.scale
        score = torch.exp(score.clamp(-5,5))
        return {'score_c': score}

    def query_by_previous_state(self, edges):
        Q, K = edges.dst['Q_p'], edges.src['K']
        score = (Q * K).sum(-1, keepdim = True)
        score = score * self.scale
        score = torch.exp(score.clamp(-5,5))
        return {'score_p': score}

    def propagate_attention(self, g):
        g.apply_edges(self.query_by_current_state)
        g.apply_edges(self.query_by_previous_state)
        eids = g.edges()

        g.send_and_recv(eids, message_func = dgl_func.u_mul_e('V', 'score_c', 'V'), reduce_func= dgl_func.sum('V', 'SV_c'))
        #g.send_and_recv(eids, message_func = dgl_fnc.copy_edge('score_c', 'score_c'), reduce_func = dgl_fnc.sum('score_c', 'z_c'))

        g.send_and_recv(eids, message_func = dgl_func.u_mul_e('V', 'score_p', 'V'), reduce_func= dgl_func.sum('V', 'SV_p'))
        #g.send_and_recv(eids, message_func = dgl_fnc.copy_edge('score_p', 'score_p'), reduce_func = dgl_fnc.sum('score_p', 'z_p'))

    def forward(self, g, h_c, h_p):
        with g.local_scope():
            Q_c, Q_p, K, V = self.Wq_c(h_c), self.Wq_p(h_p), self.Wk(h_c), self.Wv(h_c)
            Q_c, Q_p, K, V = self._reshape_qkv(Q_c, Q_p, K, V)
            g.ndata['Q_c'] = Q_c
            g.ndata['Q_p'] = Q_p
            g.ndata['K'] = K
            g.ndata['V'] = V
            self.propagate_attention(g)
            h_c = g.ndata['SV_c'] #/ (g.ndata['z_c'] + torch.full_like(g.ndata['z_c'], 1e-6))
            h_c = h_c.view(-1, self.head_dim * self.num_head)
            h_p = g.ndata['SV_p'] #/ (g.ndata['z_p'] + torch.full_like(g.ndata['z_p'], 1e-6))
            h_p = h_p.view(-1, self.head_dim * self.num_head)
            return h_c, h_p

class ST_Block(nn.Module):
    def __init__(self, in_dim, out_dim, num_head, bias = True, dropout = 0.25, norm = "LayerNorm"):
        super().__init__()
        self.dropout = dropout
        self.multihead_attention = MultiHeadAttention(in_dim, out_dim, num_head, bias)
        self.Wo_c = nn.Linear(out_dim, in_dim, bias = bias)
        self.Wo_p = nn.Linear(out_dim, in_dim, bias = bias)

        if norm == 'LayerNorm':
            self.norm_c = nn.LayerNorm(in_dim)
            self.norm_p = nn.LayerNorm(in_dim)
            self.norm_r = nn.LayerNorm(in_dim)
        else:
            self.norm_c = nn.BatchNorm1d(in_dim)
            self.norm_p = nn.BatchNorm1d(in_dim)
            self.norm_r = nn.BatchNorm1d(in_dim)

        self.mlp = nn.Sequential(
            nn.Linear(in_dim * 2, in_dim * 2),
            nn.ReLU(),
            nn.Linear(in_dim * 2, in_dim)
        )
        self.residual = nn.Linear(in_dim * 2, in_dim)

    def forward(self, g, h_c, h_p):
        h_r_c, h_r_p = h_c, h_p
        h_c, h_p = self.multihead_attention(g, h_c, h_p)
        h_c, h_p = self.Wo_c(h_c), self.Wo_p(h_p)

        h_c = self.norm_c(h_c + h_r_c)
        h_p = self.norm_p(h_p + h_r_p)

        h = torch.cat([h_c, h_p], dim = -1)
        h_r = self.residual(h)
        h = self.mlp(h)
        h = self.norm_r(h + h_r)
        return h
    
class Encoder(nn.Module):
    def __init__(self, in_dim, hidden_dim, K, dim, num_head, bias = False, norm = "LayerNorm", num_steps = 5):
        super().__init__()
        self.num_steps = num_steps
        self.blocks = nn.ModuleList([
            ST_Block(hidden_dim, dim, num_head, bias = bias, norm = norm)
            for i in range(self.num_steps)
        ])
        self.node_embedding = nn.Linear(in_dim, hidden_dim)
        self.pos_embedding = nn.Linear(K, hidden_dim)
        self.hidden_dim = hidden_dim

    def forward(self, g, H):
        # H (num_steps, num_nodes, num_features)
        lap_pos = g.ndata['lap_pos']
        lap_pos = self.pos_embedding(lap_pos)
        state = None
        for i in range(self.num_steps):
            h = H[i, :, :]
            h = self.node_embedding(h) + lap_pos
            if state is None:
                state = torch.zeros_like(h)
            state = self.blocks[i](g, h, state)
        return state


class Decoder(nn.Module):
    def __init__(self, out_dim, hidden_dim, K,dim, num_head, bias = False, norm = "LayerNorm", num_steps = 5):
        super().__init__()
        self.num_steps = num_steps
        self.blocks = nn.ModuleList([
            ST_Block(hidden_dim, dim, num_head, bias = bias, norm = norm)
            for i in range(self.num_steps)
        ])
        self.node_embedding = nn.Linear(hidden_dim, hidden_dim)
        self.pos_embedding = nn.Linear(K, hidden_dim)
        self.output_layer = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, out_dim)
        )

    def _train(self, g, encoder_output, target):
        num_node,_ = target.shape
        outputs = torch.zeros(self.num_steps, num_node,dtype = target.dtype, device = target.device)
        state = encoder_output
        h = target
        h = self.node_embedding(h) + self.lap_pos
        for i in range(self.num_steps):
            state = self.blocks[i](g, h, state)
            out = self.output_layer(state)
            outputs[i] = out.squeeze()
        return outputs

    def _infer(self, g, encoder_output, last):
        num_node, _ = last.shape
        outputs = torch.zeros(self.num_steps, num_node, dtype = last.dtype, device = last.device)
        state = encoder_output
        out = None
        for i in range(self.num_steps):
            h = self.node_embedding(last) + self.lap_pos
            state = self.blocks[i](g, h, state)
            out = self.output_layer(state)
            last = out
            outputs[i] = out.squeeze()
        return outputs

    def forward(self, g, encoder_output, target):
        lap_pos = g.ndata['lap_pos']
        self.lap_pos = self.pos_embedding(lap_pos)
        #target = target.unsqueeze(-1)
        return self._train(g, encoder_output, target)

class STGraphTransformers(nn.Module):
    def __init__(self, in_dim, hidden_dim, out_dim, K, dim, num_head, bias = False, norm = "LayerNorm", num_encode_steps = 5, num_decode_steps = 5):
        super().__init__()
        self.encoder = Encoder(in_dim, hidden_dim, K,dim, num_head, bias, norm, num_encode_steps)
        self.decoder = Decoder(out_dim ,hidden_dim, K,dim, num_head, bias, norm, num_decode_steps)

        self.readout = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.Tanh(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.Tanh(),
            nn.Linear(hidden_dim, hidden_dim)
        )
    def forward(self, g, x, target):
        state = self.encoder(g, x)
        #last = x[-1, :, 0].unsqueeze(-1) # chỉ lấy data_x[0] vì đầu vào target có dim = 1
        state = self.readout(state)
        return self.decoder(g, state, target) # last is the last x in sequence -- target is y at current
    


