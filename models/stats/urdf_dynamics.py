"""
URDF-tabanlı ters dinamik — MATLAB inverseDynamics(robot, q, qd, qdd) karşılığı.

Sadece numpy kullanır, harici kütüphane gerekmez.
Özyinelemeli Newton-Euler (RNE) algoritması — Spong, Hutchinson & Vidyasagar,
"Robot Modeling and Control" kitabındaki Algoritma 7.2'ye dayanır.

MATLAB eşdeğeri:
    robot = importrobot('robot.urdf'); robot.Gravity = [0 0 -9.81];
    tau = inverseDynamics(robot, q, qd, qdd);
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path

import numpy as np


# ── temel dönüşüm fonksiyonları ───────────────────────────────────────────────

def _rpy_to_rotation(rpy) -> np.ndarray:
    """RPY → döndürme matrisi: R = Rz(yaw) @ Ry(pitch) @ Rx(roll)  (URDF sabit eksen sırası)."""
    r, p, y = float(rpy[0]), float(rpy[1]), float(rpy[2])
    cr, sr = np.cos(r), np.sin(r)
    cp, sp = np.cos(p), np.sin(p)
    cy, sy = np.cos(y), np.sin(y)
    Rx = np.array([[1, 0,  0 ], [0, cr, -sr], [0,  sr, cr]])
    Ry = np.array([[cp, 0, sp], [0, 1,  0  ], [-sp, 0, cp]])
    Rz = np.array([[cy, -sy, 0], [sy, cy, 0], [0, 0, 1]])
    return Rz @ Ry @ Rx


def _rotz(q: float) -> np.ndarray:
    """Z ekseni etrafında q (rad) döndürme matrisi."""
    cq, sq = np.cos(q), np.sin(q)
    return np.array([[cq, -sq, 0.0], [sq, cq, 0.0], [0.0, 0.0, 1.0]])


# ── URDF ayrıştırıcı ──────────────────────────────────────────────────────────

@lru_cache(maxsize=4)
def _parse_urdf(urdf_path: str):
    """
    URDF'den eklem ve halka ataletsel verilerini çıkarır.
    Dönüş: (joints, links)
      joints[i]: {'xyz': ndarray(3), 'rpy': ndarray(3)}    — link i'nin bağlantı orijini
      links[i] : {'mass': float, 'com': ndarray(3), 'I': ndarray(3,3)}  — link i ataletsel verileri
    """
    path = Path(urdf_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"URDF bulunamadı: {path}")

    tree = ET.parse(str(path))
    root = tree.getroot()

    # Zincir sırası: link_1 → link_2 → ... → link_6
    chain = [f"link_{i}" for i in range(1, 7)]

    # Eklem bilgileri: child link adına göre dizine alınmış
    joint_by_child = {}
    for j in root.findall("joint"):
        if j.get("type") in ("revolute", "prismatic"):
            child = j.find("child").get("link")
            joint_by_child[child] = j

    joints = []
    for link_name in chain:
        j = joint_by_child[link_name]
        origin = j.find("origin")
        xyz_str = origin.get("xyz", "0 0 0") if origin is not None else "0 0 0"
        rpy_str = origin.get("rpy", "0 0 0") if origin is not None else "0 0 0"
        joints.append({
            "xyz": np.array(list(map(float, xyz_str.split()))),
            "rpy": np.array(list(map(float, rpy_str.split()))),
        })

    # Link ataletsel verileri
    link_map = {l.get("name"): l for l in root.findall("link")}
    links = []
    for link_name in chain:
        lnk = link_map[link_name]
        inertial = lnk.find("inertial")
        mass = float(inertial.find("mass").get("value"))
        orig = inertial.find("origin")
        com_xyz = list(map(float, (orig.get("xyz", "0 0 0") if orig is not None else "0 0 0").split()))
        com_rpy = list(map(float, (orig.get("rpy", "0 0 0") if orig is not None else "0 0 0").split()))
        inert = inertial.find("inertia")
        ixx = float(inert.get("ixx", 0))
        iyy = float(inert.get("iyy", 0))
        izz = float(inert.get("izz", 0))
        ixy = float(inert.get("ixy", 0))
        ixz = float(inert.get("ixz", 0))
        iyz = float(inert.get("iyz", 0))
        I_body = np.array([[ixx, ixy, ixz],
                           [ixy, iyy, iyz],
                           [ixz, iyz, izz]])
        # Atalet tensörünü CoM çerçevesinden halka çerçevesine döndür
        # I_link = R_com @ I_body @ R_com^T  (sütunlar = CoM eksenleri, halka çerçevesinde)
        R_com = _rpy_to_rotation(com_rpy)
        I_link = R_com @ I_body @ R_com.T
        links.append({
            "mass": mass,
            "com":  np.array(com_xyz),
            "I":    I_link,
        })

    return joints, links


# ── tek satır ters dinamik ────────────────────────────────────────────────────

def compute_inverse_dynamics(
    urdf_path: str,
    q:   np.ndarray,
    qd:  np.ndarray,
    qdd: np.ndarray,
    gravity: np.ndarray | None = None,
) -> np.ndarray:
    """
    Özyinelemeli Newton-Euler ters dinamiği.

    q, qd, qdd : (6,)  rad, rad/s, rad/s²
    gravity    : (3,)  yer çekimi vektörü [m/s²], varsayılan [0, 0, -9.81]
    Dönüş      : tau (6,)  [Nm]

    MATLAB eşdeğeri:
        robot.Gravity = [0 0 -9.81];
        tau = inverseDynamics(robot, q, qd, qdd);
    """
    if gravity is None:
        gravity = np.array([0.0, 0.0, -9.81])

    joints, links = _parse_urdf(urdf_path)
    n = len(joints)   # 6
    z = np.array([0.0, 0.0, 1.0])

    # Her eklem için döndürme matrisi:
    # R_pc[i] sütunları = link i eksenlerinin link i-1 çerçevesindeki koordinatları
    # yani: v_{i-1} = R_pc[i] @ v_i   (çocuktan ebeveyne)
    #       v_i     = R_pc[i].T @ v_{i-1}  (ebeveynden çocuğa)
    R_pc = [
        _rpy_to_rotation(joints[i]["rpy"]) @ _rotz(float(q[i]))
        for i in range(n)
    ]

    # ── İLERİ ÖZYINELEME (ebeveynden çocuğa, her şey link i çerçevesinde) ────
    omega   = np.zeros((n, 3))   # açısal hız
    alpha_a = np.zeros((n, 3))   # açısal ivme
    a_o     = np.zeros((n, 3))   # eklem orijininin doğrusal ivmesi
    a_c     = np.zeros((n, 3))   # kütle merkezinin doğrusal ivmesi

    # Başlangıç koşulları (taban çerçeve = world)
    omega_prev = np.zeros(3)
    alpha_prev = np.zeros(3)
    a_o_prev   = -gravity   # yer çekimini dengelemek için: a_0 = -g = [0, 0, 9.81]

    for i in range(n):
        T = R_pc[i].T          # link i-1 → link i dönüşümü
        p = joints[i]["xyz"]   # eklem i orijini, link i-1 çerçevesinde

        # Açısal hız: Spong Eq. 7.127
        omega[i] = T @ omega_prev + float(qd[i]) * z

        # Açısal ivme: Spong Eq. 7.128
        alpha_a[i] = (T @ alpha_prev
                      + float(qdd[i]) * z
                      + float(qd[i]) * np.cross(T @ omega_prev, z))

        # Eklem orijininin doğrusal ivmesi: Spong Eq. 7.129
        a_o[i] = T @ (
            np.cross(alpha_prev, p)
            + np.cross(omega_prev, np.cross(omega_prev, p))
            + a_o_prev
        )

        # Kütle merkezinin doğrusal ivmesi: Spong Eq. 7.130
        rc = links[i]["com"]
        a_c[i] = (np.cross(alpha_a[i], rc)
                  + np.cross(omega[i], np.cross(omega[i], rc))
                  + a_o[i])

        omega_prev = omega[i]
        alpha_prev = alpha_a[i]
        a_o_prev   = a_o[i]

    # ── GERİ ÖZYINELEME (çocuktan ebeveyne, her şey link i çerçevesinde) ─────
    tau    = np.zeros(n)
    f_next = np.zeros(3)   # uç efektörden sonrası: dış kuvvet yok
    n_next = np.zeros(3)   # uç efektörden sonrası: dış moment yok

    for i in range(n - 1, -1, -1):
        mass = links[i]["mass"]
        I    = links[i]["I"]
        rc   = links[i]["com"]

        # Newton ve Euler denklemleri: Spong Eq. 7.131-7.132
        F = mass * a_c[i]
        N = I @ alpha_a[i] + np.cross(omega[i], I @ omega[i])

        # Sonraki halkadan kuvvet ve momentleri bu hanka çerçevesine dönüştür
        if i < n - 1:
            # R_pc[i+1]: link i+1 → link i dönüşümü (sütunlar = link i+1 eksenlerinin link i'deki koordinatları)
            f_in = R_pc[i + 1] @ f_next
            n_in = R_pc[i + 1] @ n_next
            p_next = joints[i + 1]["xyz"]   # eklem i+1 orijini, link i çerçevesinde
        else:
            f_in   = np.zeros(3)
            n_in   = np.zeros(3)
            p_next = np.zeros(3)

        # Kuvvet ve moment dengesi: Spong Eq. 7.133a-b
        f_curr = f_in + F
        n_curr = N + n_in + np.cross(rc, F) + np.cross(p_next, f_in)

        # Eklem torku: Spong Eq. 7.133c
        tau[i] = float(np.dot(n_curr, z))

        f_next = f_curr
        n_next = n_curr

    return tau


# ── toplu ters dinamik ────────────────────────────────────────────────────────

def compute_inverse_dynamics_batch(
    urdf_path: str,
    q:   np.ndarray,
    qd:  np.ndarray,
    qdd: np.ndarray,
    gravity: np.ndarray | None = None,
) -> np.ndarray:
    """
    Toplu ters dinamik — MATLAB'daki döngüye karşılık:
        for idx = 1:N
            tau(idx,:) = inverseDynamics(robot, q(idx,:)', qd(idx,:)', qdd(idx,:)')';
        end

    q, qd, qdd : (N, 6)
    Dönüş      : tau_model (N, 6) [Nm]
    """
    if gravity is None:
        gravity = np.array([0.0, 0.0, -9.81])

    N = q.shape[0]
    tau = np.zeros((N, 6), dtype=float)

    for i in range(N):
        try:
            tau[i] = compute_inverse_dynamics(urdf_path, q[i], qd[i], qdd[i], gravity)
        except Exception:
            tau[i] = 0.0

    return tau
